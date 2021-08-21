from typing import Any, NoReturn, Callable, Dict, List
import os


def uncomment(line: str):
    if not line.lstrip().startswith("<!--"):
        return line
    return line.rstrip("\n").lstrip().lstrip("<!--").rstrip().rstrip("\n").rstrip("-->") + "\n"


def comment(line: str):
    if line.lstrip().startswith("<!--"):
        return str
    return "<!-- " + line.rstrip("\n") + " -->" + "\n"


def apply(path: str, target: str) -> NoReturn:
    newlines = []

    meet2_4 = False
    meet3_0 = False

    with open(path, "r") as reader:
        line = reader.readline()
        while line:

            if "<!-- spark 2.4 end -->" in line:
                meet2_4 = False

            if meet2_4:
                if target == "2.4":
                    newlines.append(uncomment(line))
                if target == "3.0":
                    newlines.append(comment(line))

            if "<!-- spark 2.4 start -->" in line:
                newlines.append(line)
                meet2_4 = True

            if "<!-- spark 3.0 end -->" in line:
                meet3_0 = False

            if meet3_0:
                if target == "2.4":
                    newlines.append(comment(line))
                if target == "3.0":
                    newlines.append(uncomment(line))

            if "<!-- spark 3.0 start -->" in line:
                newlines.append(line)
                meet3_0 = True

            if not meet2_4 and not meet3_0:
                newlines.append(line)

            line = reader.readline()
    with open(path, "w") as writer:
        writer.writelines(newlines)


for root, dirs, files in os.walk("."):
    for file in files:
        if file.endswith("pom.xml"):
            file_path = os.path.join(root, file)
            print(f"Apply {file_path}")
            apply(file_path)
