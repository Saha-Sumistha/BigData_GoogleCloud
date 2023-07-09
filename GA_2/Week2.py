import requests

def hello_world(i):
  response = requests.get('https://storage.googleapis.com/sumisthasaha_ga2/week1.txt')
  x = response.text
  count = 0
  for i in x.split("\n"):
    count += 1
  return "Number of lines is : " + str(count) + "\n"