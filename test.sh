#!/bin/bash
# In another terminal run this:
# . ./test.sh
# build && calc
URL=http://0.0.0.0:3000
echo "URL: $URL"
CALLBACK_URL=$URL/test/push
STACK=$(/bin/which stack)
stack(){
  "$STACK" --docker "$@"
}
post(){
  local t="$1"
  local p="$2"
  shift 2
  if [ "$#" = 1 ]; then
    case "$1" in
    (-*) : ;;
    (*) set -- -d"$1" ;;
    esac
  fi
  case "$t" in
  (json) set -- -H 'Content-Type: application/json' "$@" ;;
  (form) ;;
  (*)
    echo "Usage post [json|form] <URL> <CURL_OPT>*"
    ;;
  esac
  curl -s -XPOST "$@" "$URL/$p"
}
get(){
  curl -s "$URL/$1"
}
sync(){
  local c="$1"
  local p="$2"
  shift 2
  post "$c" "sync/$p" "$@"
}
async(){
  local t="$1"
  local q="$2"
  shift 2
  local p="$(echo "$q" | cut -d'?' -f1)"
  local task="$(post "$t" "async/$q" "$@" | jq -r .id)"
  get  "async/$p/$task/wait"
  rm -f test.poll.log test.kill.log 2> /dev/null
  get  "async/$p/$task/poll" > test.poll.log
  post json "async/$p/$task/kill" > test.kill.log
}
callback(){
  local c="$1"
  local p="$2"
  shift 2
  post form test/clear
  case "$c" in
  (json)
    set -- -d"{\"input\": $1, \"callback\": \"$CALLBACK_URL\"}"
    ;;
  (form)
    set -- -d callback="$CALLBACK_URL" "$@"
    ;;
  esac
  res1="$(async "$c" "$p" "$@")"
  res2="$(get test/pull | jq -c '{output: first(.[] | select(.[0][0] == "output")[1])}')"
  if [ "$res1" = "$res2" ]; then
    echo "$res1"
  else
    echo "  The test returned:"
    jq -Rr '"   |" + .' <<<"$res1"
    echo "  But the following was expected:"
    jq -Rr '"   |" + .' <<<"$res2"
  fi
}
stream(){
  local t="$1"
  local q="$2"
  shift 2
  local p="$(echo "$q" | cut -d'?' -f1)"
  local a="$(echo "$q" | cut -d'?' -f2)"
  if [ "$p?$a" = "$q" ]; then
    :
  else
    echo "Diff $p?$a and $q" >>/dev/stderr
  fi
  post "$t" "stream/$p?$a" "$@"
}
select_output(){
  tee -a check_output.log |
  jq -c 'select(.output).output'
}
successes=0
failures=0
check(){
  local my="$(eval "$1")"
  local ref="$2"
  if [ $# != 2 ]; then
    echo "ERROR: two arguments expected"
  fi
  if [ "$my" = "$ref" ]; then
    successes=$((successes + 1))
    echo -n "PASS: \`$1\` = "
    jq -RsaM . <<<"$my"
  else
    failures=$((failures + 1))
    echo "FAIL: \`$1\`"
    echo "  The test returned:"
    jq -Rr '"   |" + .' <<<"$my"
    echo "  But the following was expected:"
    jq -Rr '"   |" + .' <<<"$ref"
  fi
}
check_output(){
  check "$1 | select_output" "$2"
}
test_stream(){
  check "stream json 'sum?delay=0.1' '[1,2,3,4,5]'" "$(
  cat <<EOF
{"event":0}
{"event":5}
{"event":9}
{"event":12}
{"event":14}
{"event":15}
{"output":15}
EOF
)"
}
test_sum(){
  for m in sync async stream callback; do
    check_output "$m json sum '[1,2,3,4,5]'" 15
    check_output "$m form sum -d 'x=1&x=2&x=3&x=4&x=5'" 15
  done
}
test_product(){
  for m in sync async stream callback; do
    check_output "$m json product '[1,2,3,4,5]'" 120
    check_output "$m form product -d x=1 -d x=2 -d x=3 -d x=4 -d x=5" 120
  done
}
test_polynomial_(){
  for m in sync async stream callback; do
    check_output "$m '$1'  polynomial $2" "$3"
    check_output "$m '$1' 'polynomial?pure=1' $2" "$3"
    check_output "$m '$1' 'polynomial?sum=async' $2" "$3"
    check_output "$m '$1' 'polynomial?product=async' $2" "$3"
    check_output "$m '$1' 'polynomial?sum=async&product=async' $2" "$3"
    check_output "$m '$1' 'polynomial?sum=callback' $2" "$3"
    check_output "$m '$1' 'polynomial?product=callback' $2" "$3"
    check_output "$m '$1' 'polynomial?sum=callback&product=callback' $2" "$3"
    check_output "$m '$1' 'polynomial?sum=async&product=callback' $2" "$3"
    check_output "$m '$1' 'polynomial?sum=callback&product=async' $2" "$3"
  done
}
test_polynomial(){
  test_polynomial_ json "'[[1,2], 3]'" 7
  test_polynomial_ json "'{\"c\": [1,2,3,4,5], \"x\": 3}'" 547
  test_polynomial_ form '-d c=1 -d c=2 -d c=3 -d c=4 -d c=5 -d x=3' 547
}
test_callback(){
  check_output "sync json 'polynomial?sum=callback' '[[1,2], 3]'" 7
  check_output "sync json 'polynomial?product=callback' '[[1,2], 3]'" 7
  check_output "sync json 'polynomial?sum=callback&product=callback' '[[1,2], 3]'" 7
}
build(){
  stack build --pedantic
}
calc(){
  stack exec servant-job-calc-example 3000
}
scrapy(){
  stack exec servant-job-scrapy-example 24000 https://dev.gargantext.org:8080
}
test_scrapy(){
  async json scrapy '{ "spider": "pubmed", "query": "cancer colon", "user": "bob", "corpus": 4757 }'
}

if [[ "${BASH_SOURCE[0]}" = "${0}" ]]; then
  set -e
  exec 2>/dev/null
  test_stream
  test_sum
  test_product
  test_polynomial
  echo
  echo "==============="
  echo "=== Summary ==="
  echo "==============="
  echo
  echo "$failures failures"
  echo "$successes successes"
fi
