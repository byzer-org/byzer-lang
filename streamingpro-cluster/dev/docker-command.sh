#!/usr/bin/env bash

function docker_exec {
  name=$1
  command=$2
  docker exec -it ${name} bash -c "${command}"
}

function check_ready {
    recCode="1"
    counter=0
    name=$1
    command=$2
    while [[ $counter -lt 60 ]] ; do
      docker_exec "${1}" "${command}"
      recCode="$?"
      if [[ "$recCode" == "0" ]]; then
          echo "check success. try ${counter} times."
          break
      fi
      sleep 1
      echo "try ${counter} times."
      let "counter+=1"
    done
    return $recCode
}