file = open("user.dic")
result = open("result.dic", mode="+w")
for line in file:
    result.write(line.strip() + "\t" + "nr" + "\t" + "1000" + "\n")
file.close()
result.close()