#!/bin/bash

# chmod u+x cli.sh
# ./cli.sh

VALID_CMD="exit,get,set,del,exists,size";
VERIFY_RESULT=0;
EXEC_RESULT="参数错误，允许命令：$VALID_CMD";

function operation() {
  #display $1;
  verify $1;
  if [[ $VERIFY_RESULT -eq -1 ]] ; then
    EXEC_RESULT="无效操作：[${array[0]}]（有效值：$VALID_CMD）";
    return 0;
  fi;
  if [[ $VERIFY_RESULT -eq 1 ]] ; then
    EXEC_RESULT="";
    return 0;
  fi;
  array=$1;
  param_size=${#array[@]};
  if [[ param_size -eq 1 ]]; then
    if [[ ${array[0]} == 'exit' ]]; then
       exit 0;
    fi;
    EXEC_RESULT="[${array[0]}]参数不能为空";
  elif [[ ${array[0]} == 'set' ]]; then
    if [[ param_size -eq 2 ]]; then
      EXEC_RESULT="[set]操作value不能为空";
      return 0;
    elif [[ param_size -eq 3 ]]; then
      executor ${array[0]} ${array[1]} ${array[2]};
      return 0;
    fi;
  elif [[ param_size -eq 2 ]]; then
    executor ${array[0]} ${array[1]} ${array[1]};
    return 0;
  fi;
  EXEC_RESULT="[${array[0]}]参数过多";
}

# $?
function verify() {
    array=$1;
    param_size=${#array[@]};
    if [[ param_size -eq 0 ]]; then
      VERIFY_RESULT=-1;
      return 0;
    fi;
    if [[ ${array[0]} == '' ]]; then
      VERIFY_RESULT=1;
      return 0;
    fi;
    if [[ $VALID_CMD =~ ${array[0]} ]]; then
      VERIFY_RESULT=0;
    else
      VERIFY_RESULT=-1;
      return 0;
    fi;
    VERIFY_RESULT=0;
}

function display() {
  array=$1;
  param_size=${#array[@]}; # ${#names[*]}
  echo "【参数个数】：$param_size";
  for ele in ${array[@]};
  do
  echo "【数据展示】：${ele}"
  done;
}

function executor() {
  cmd=$1;
  key=$2;
  val=$3;
  EXEC_RESULT=`curl -s http://127.0.0.1:6379/$cmd?$key=$val`;
}

while :;
do
  echo -n "XiKV 127.0.0.1:6379 > ";
  read -a array;
  operation $array;
  echo $EXEC_RESULT;
done;
