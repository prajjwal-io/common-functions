from difflib import SequenceMatcher
a = "Adobe"
b= "Adobe Captivate Prime"
c = "3CX"
d= "3CX Phone System"
e = "my steryttle oke cards"
f = "mystery"
g = "Javascript"
h= "Java Script"

#ratio = SequenceMatcher(None,a, b).ratio()
#ratio = SequenceMatcher(None,c, d).ratio()
ratio = SequenceMatcher(None,e, f).ratio()
#ratio = SequenceMatcher(None,g, h).ratio()
print(ratio)