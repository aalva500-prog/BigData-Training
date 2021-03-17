# -*- coding: utf-8 -*-
"""
Created on Thu Feb 18 13:20:18 2021

@author: Aaron Alvarez
"""

# Working with lambda functions (a lambda function is an anonymous function, meaning that it does'n have a name)

list_numbers = [1,2,3,4,5]

add_number = list(map(lambda num: (num + 3), list_numbers))

print(add_number)
        
words = []

with open('book.txt', 'r') as file:
    for line in file:
        w = line.split()
        words.append(w)
        
list_words = list(map(lambda w: w, words))

print(list_words)
