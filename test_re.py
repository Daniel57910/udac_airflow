import re
string = '3514.968;; ;'

find_string = r'([^\s\w\d.])'

out = re.sub(find_string, '', string)
print(out)