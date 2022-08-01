inputdate = input('input like "2019-08-25":').split('-')
# inputdate = "2020-12-31".split('-')

year = inputdate[0]
real_month = int(inputdate[1])
if int(year) % 4 == 0:
    feb = 29
else:
    feb = 28
month = [31, feb, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
day = int(inputdate[2])

days = 0
for i in range(real_month - 1):
    days = days + month[i]
days = days + day

print('{}D{}'.format(year, str(days)))
