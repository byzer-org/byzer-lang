from typing import Any, NoReturn, Callable, Dict, List
import os


def apply(path: str) -> NoReturn:
    newlines = []

    meet = False
    meet1 = False

    with open(path, "r") as reader:
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
    with open(path, "w") as writer:
        writer.writelines(newlines)


for root, dirs, files in os.walk("."):
    for file in files:
        if file.endswith("pom.xml"):
            file_path = os.path.join(root, file)
            print(f"Apply {file_path}")
            apply(file_path)
