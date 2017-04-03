def getS(r, b):
    e1 = (0.5)**(1.0/b)
    return (1.0-e1)**(1.0/r)

print(getS(3.0,10.0))
print(getS(6.0,20.0))
print(getS(5.0,50.0))

