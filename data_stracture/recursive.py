def fact(n):
    # factorial
    if n == 0:
        return 1
    else:
        return n*fact(n-1)


def rec_sum(n):
    # recursive sum
    if n == 0:
        return 0
    else:
        return n + rec_sum(n - 1)


def sum_func(n):
    # sum of all the digits
    # Note: int doesn't have len()
    if len(str(n)) == 1:
        return n
    else:
        return n % 10 + sum_func(n//10)


def split_func(phrase, word, output):
    if phrase.startswith(word):
        output.append(word)


def word_split(phrase, list_of_words):
    output = []
    for word in list_of_words:
        split_func(phrase, word, output)
        phrase = phrase[len(word):]
    return output


# factorial
print(fact(4))
print(fact(0))

# recursive sum
print(rec_sum(4))
print(rec_sum(0))

# sum of all digits
print(sum_func(4321))
print(sum_func(0))

print(word_split("ilovecat", ['i','love','cat']))
print(word_split("doglovecat", ['i','love','cat']))
