import requests

response=requests.get('https://storage.googleapis.com/sumisthasaha_ga1/week1.txt')
x = response.text
count = 0
for i in x.split("\n"):
  count += 1
f = open('output.txt', 'w')
f.write('Total no of lines : ' + str(count) + '\n')
f.close()