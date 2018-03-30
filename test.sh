#!/bin/bash
# In another terminal run this:
# . ./test.sh
# build && calc
URL=http://0.0.0.0:3000
CALLBACK_URL=$URL/test/push
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
  echo
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
  post "$c" "callback/$p" "$@" >/dev/null
  sleep 1 # TODO
  get test/pull | jq 'first(.[] | select(.[0][0] == "output")[1])'
}
async(){
  local t="$1"
  local q="$2"
  shift 2
  local p="$(echo "$q" | cut -d'?' -f1)"
  local task="$(post "$t" "async/$q" "$@" | jq -r .id)"
  get  "async/$p/$task/wait"
  echo
  rm -f test.poll.log test.kill.log 2> /dev/null
  get  "async/$p/$task/poll" > test.poll.log
  post json "async/$p/$task/kill" > test.kill.log
}
stream(){
  local t="$1"
  local p="$2"
  shift 2
  rm -f test.stream.json 2> /dev/null
  post "$t" "stream/$p" "$@" > test.stream.json
  jq '.output | select(.)' < test.stream.json
}
successes=0
failures=0
check(){
  local my="$(eval "$1")"
  if [ $# != 2 ]; then
    echo "ERROR: two arguments expected"
  fi
  if [ "$my" = "$2" ]; then
    successes=$((successes + 1))
    echo "PASS: \`$1\` = $my"
  else
    failures=$((failures + 1))
    echo "FAIL: \`$1\` = $my but $2 was expected"
  fi
}
test_sum(){
  for m in sync async stream callback; do
    check "$m json sum '[1,2,3,4,5]'" 15
    check "$m form sum -d 'x=1&x=2&x=3&x=4&x=5'" 15
  done
}
test_product(){
  for m in sync async stream callback; do
    check "$m json product '[1,2,3,4,5]'" 120
    check "$m form product -d x=1 -d x=2 -d x=3 -d x=4 -d x=5" 120
  done
}
test_polynomial_(){
  for m in sync async stream callback; do
    check "$m '$1'  polynomial $2" "$3"
    check "$m '$1' 'polynomial?pure=1' $2" "$3"
    check "$m '$1' 'polynomial?sum=async' $2" "$3"
    check "$m '$1' 'polynomial?product=async' $2" "$3"
    check "$m '$1' 'polynomial?sum=async&product=async' $2" "$3"
    check "$m '$1' 'polynomial?sum=callback' $2" "$3"
    check "$m '$1' 'polynomial?product=callback' $2" "$3"
    check "$m '$1' 'polynomial?sum=callback&product=callback' $2" "$3"
    check "$m '$1' 'polynomial?sum=async&product=callback' $2" "$3"
    check "$m '$1' 'polynomial?sum=callback&product=async' $2" "$3"
  done
}
test_polynomial(){
  test_polynomial_ json "'[[1,2], 3]'" 7
  test_polynomial_ json "'{\"c\": [1,2,3,4,5], \"x\": 3}'" 547
  test_polynomial_ form '-d c=1 -d c=2 -d c=3 -d c=4 -d c=5 -d x=3' 547
}
test_callback(){
  check "sync json 'polynomial?sum=callback' '[[1,2], 3]'" 7
  check "sync json 'polynomial?product=callback' '[[1,2], 3]'" 7
  check "sync json 'polynomial?sum=callback&product=callback' '[[1,2], 3]'" 7
}
build(){
  stack build --pedantic
}
calc(){
  stack exec servant-async-calc-example 3000
}
scrapy(){
  stack exec servant-async-scrapy-example 24000 https://dev.gargantext.org:8080
}
test_scrapy(){
  async json scrapy '{ "spider": "pubmed", "query": "cancer colon", "user": "bob", "corpus": 4757 }'
}

if [[ "${BASH_SOURCE[0]}" = "${0}" ]]; then
  set -e
  exec 2>/dev/null
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
