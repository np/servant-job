set -e
# In another terminal run this:
# stack build && stack exec servant-async-calc-example &
URL=0.0.0.0:3000
post(){
  curl -H 'Content-Type: application/json' -s -d@- -XPOST "$URL/$1"
}
get(){
  curl -s "$URL/$1"
}
testsyncsum(){
  echo '[1,2,3,4,5]' | post "sync/sum"
  echo
}
testsyncproduct(){
  echo '[1,2,3,4,5]' | post "sync/product"
  echo
}
testasyncsum(){
  task=$(echo '[1,2,3,4,5]' | post "async/sum" | jq -r .id)
  get "async/sum/$task/wait"
  echo
}
testasyncpolynomial(){
  task=$(echo '[[1,2,3,4,5], 3]' | post "async/polynomial" | jq -r .id)
  get "async/polynomial/$task/wait"
  echo
}

testsyncsum
testsyncproduct
testasyncsum
testasyncpolynomial
