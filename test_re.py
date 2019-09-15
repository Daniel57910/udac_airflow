import re
string = 'Cienna,F,Freeman'
find_string = r'([^\s\w\d.])'

out = re.sub(find_string, '', string)
print(out)
