# How to determine the full exception object's name to handle in the except clause?  ANSWER: use type() in the except clause on the Exception object:

```{python}
try:
    response = requests.get(url, timeout=10)
    response.raise_for_status()
except Exception as e:
    print(type(e))            # <class 'requests.exceptions.HTTPError'>
    print(type(e).__module__) # requests.exceptions
    print(type(e).__mro__)    # full inheritance chain
```