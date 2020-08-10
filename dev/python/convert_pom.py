newlines = []

meet = False
meet1 = False

with open("pom.xml", "r") as reader:
    line = reader.readline()
    while line:

        if "<!-- spark 2.4 end -->" in line:
            meet = False

        if meet:
            newlines.append("<!-- " + line.rstrip("\n") + " -->" + "\n")

        if "<!-- spark 2.4 start -->" in line:
            newlines.append(line)
            meet = True

        if "<!-- spark 3.0 end -->" in line:
            meet1 = False
            
        if meet1:
            newlines.append(line.lstrip().lstrip("<!--").rstrip().rstrip("\n").rstrip("-->") + "\n")

        if "<!-- spark 3.0 start -->" in line:
            newlines.append(line)
            meet1 = True
            
        if not meet and not meet1:
            newlines.append(line)

        line = reader.readline()

# for line in newlines:
#     print(line)
with open("pom.xml","w") as writer:
    writer.writelines(newlines)
