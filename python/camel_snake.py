txt="camel Case"
def change(txt):
    result=""
    for i in txt:
        if i.islower():
            result=result+i
        elif i==" ":
            pass
        else:
            result=result+"_"+i.lower()
    return result

print(change(txt))