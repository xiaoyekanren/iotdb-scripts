#!/bin/bash

# 当前拼出来的get语句

# ## Create schema cost
# cat 1.out |grep "\-Result Matrix\-" -B 2 | head -n 1 | cut -d ' ' -f 4

# ## Test elapsed time
# cat 1.out |grep "\-Result Matrix\-" -B 1 | head -n 1 | cut -d ' ' -f 8

## 


abc="1:0:0:0:0:0:0:0:0:0:0"
b=1
while (( $b <= `echo $abc | awk '{n=split($0, array, ":")} END{print n}'` )); do
def=$(echo $abc | cut -d ':' -f $b)
let 'b+=1'
echo $def
done
