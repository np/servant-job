#!/bin/bash
# In another terminal run this:
# stack build && stack exec servant-async-calc-example &
URL=0.0.0.0:3000
post(){
  if [ -n "$2" ]; then
    curl -H 'Content-Type: application/json' -s -d"$2" -XPOST "$URL/$1"
  else
    curl -s -XPOST "$URL/$1"
  fi
}
get(){
  curl -s "$URL/$1"
}
sync(){
  post "sync/$1" "$2"
  echo
}
async(){
  task="$(post "async/$1" "$2" | jq -r .id)"
  get  "async/$1/$task/wait"
  echo
  get  "async/$1/$task/poll" >&2
  post "async/$1/$task/kill" >&2
}
check(){
  local my="$(eval "$1")"
  if [ "$my" = "$2" ]; then
    echo "PASS: $1"
  else
    echo "FAIL: $1 = $my but $2 was expected"
  fi
}
test_sum(){
  check "sync sum '[1,2,3,4,5]'" 15
  check "async sum '[1,2,3,4,5]'" 15
}
test_product(){
  check "sync product '[1,2,3,4,5]'" 120
  check "async product '[1,2,3,4,5]'" 120
}
test_polynomial(){
  check "sync  polynomial '[[1,2,3,4,5], 3]'" 547
  check "sync 'polynomial?pure=1' '[[1,2,3,4,5], 3]'" 547
  check "sync 'polynomial?sum=async' '[[1,2,3,4,5], 3]'" 547
  check "sync 'polynomial?product=async' '[[1,2,3,4,5], 3]'" 547
  check "sync 'polynomial?sum=async&product=async' '[[1,2,3,4,5], 3]'" 547
  check "async polynomial '[[1,2,3,4,5], 3]'" 547
# TODO
# check "async 'polynomial?pure=1' '[[1,2,3,4,5], 3]'" 547
# check "async 'polynomial?sum=async' '[[1,2,3,4,5], 3]'" 547
# check "async 'polynomial?product=async' '[[1,2,3,4,5], 3]'" 547
# check "async 'polynomial?sum=async&product=async' '[[1,2,3,4,5], 3]'" 547
}

if [[ "${BASH_SOURCE[0]}" = "${0}" ]]; then
  set -e
  exec 2>/dev/null
  test_sum
  test_product
  test_polynomial
fi
