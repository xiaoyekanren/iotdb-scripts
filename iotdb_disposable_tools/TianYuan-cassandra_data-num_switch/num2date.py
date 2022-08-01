inputdate = input('input like "2019D255":')
# inputdate = "2019D255"

year = int(inputdate[0:4])
if year % 4 == 0:
    feb = 29
else:
    feb = 28
month = [31, feb, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
day = int(inputdate[5:])

i = 0
while day > 0:
    day = day - month[i]
    i += 1

# print(str(year) + '-' + str(i) + '-' + str(day+month[i-1]))
print('{}-{}-{}'.format(str(year), str(i), str(day + month[i - 1])))
