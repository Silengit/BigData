file = open("people_name_list.txt")
result = open("result.txt",mode="+w")
for i, name in enumerate(file, 0):
    result.write(name.strip() + "\t" + str(i) + "\n")
result.close()
file.close()