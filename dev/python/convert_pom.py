import re
import sys
from typing import Any, NoReturn, Callable, Dict, List
import os


def uncomment(line: str):
    if not line.lstrip().startswith("<!--"):
        return line
    return line.rstrip("\n").lstrip().lstrip("<!--").rstrip().rstrip("\n").rstrip("-->") + "\n"


def comment(line: str):
    if line.lstrip().startswith("<!--"):
        return line
    return "<!-- " + line.rstrip("\n") + " -->" + "\n"


def cleanLine(line: str) -> str:
    return line.rstrip("\n").strip()


def find(lines: List[str], start: int, start_tag: str, end_tag: str, stop_tag: str) -> (int, int):
    _start = start
    start_tag_index = -1
    end_tag_index = -1
    while _start < len(lines):
        if cleanLine(lines[_start]) == "":
            _start += 1
            continue
        if cleanLine(lines[_start]) == stop_tag:
            break
        if cleanLine(lines[_start]) == start_tag and end_tag_index == -1:
            start_tag_index = _start

        if cleanLine(lines[_start]) == end_tag and start_tag != -1:
            end_tag_index = _start
        _start += 1
    return start_tag_index, end_tag_index


def c_activation(path: str, target: str) -> NoReturn:
    mapping = {
        "scala-2.11": "2.4",
        "scala-2.12": "3.0",
        "spark-2.4.0": "2.4",
        "spark-3.0.0": "3.0",
        "streamingpro-spark-2.4.0-adaptor": "2.4",
        "streamingpro-spark-3.0.0-adaptor": "3.0"
    }
    with open(path, "r") as reader:
        lines = reader.readlines()

        # scala-2.11 scala-2.12
        # spark-2.4.0 spark-3.0.0
        # streamingpro-spark-2.4.0-adaptor streamingpro-spark-3.0.0-adaptor
        def find_target(lines: List[str], name):
            index = 0
            s = -1
            e = -1
            while index < len(lines):
                if index < 4:
                    index += 1
                    continue
                if f"<id>{name}</id>" in lines[index]:
                    start_tag_index, end_tag_index = find(lines, index, "<activation>", "</activation>", "</profile>")
                    # if no <activation> found, and we should add ,then set add pos
                    if start_tag_index == -1 and target == mapping[name]:
                        s = index
                        break
                    # if <activation> is found ,and we should remove, then set remove pos
                    if start_tag_index != -1 and end_tag_index != -1 and target != mapping[name]:
                        s = start_tag_index
                        e = end_tag_index
                        break
                index += 1
            return s, e

        def add_replace(lines: List[str], s: int, e: int):

            if s == -1 and e == -1:
                return lines

            newlines = []
            # add
            if s != -1 and e == -1:
                for i in range(len(lines)):
                    newlines.append(lines[i])
                    if i == s:
                        newlines.append('''
                <activation>
                    <activeByDefault>true</activeByDefault>
                </activation>
''')

            # replace
            if s != -1 and e != -1:
                for i in range(len(lines)):
                    if i < s or i > e:
                        newlines.append(lines[i])
            return newlines

        s, e = find_target(lines, "scala-2.11")
        newlines = add_replace(lines, s, e)

        s, e = find_target(newlines, "scala-2.12")
        newlines = add_replace(newlines, s, e)

        s, e = find_target(newlines, "spark-2.4.0")
        newlines = add_replace(newlines, s, e)

        s, e = find_target(newlines, "spark-3.0.0")
        newlines = add_replace(newlines, s, e)


        s, e = find_target(newlines, "streamingpro-spark-2.4.0-adaptor")
        newlines = add_replace(newlines, s, e)

        s, e = find_target(newlines, "streamingpro-spark-3.0.0-adaptor")
        newlines = add_replace(newlines, s, e)

        with open(path, "w") as writer:
            writer.writelines(newlines)


def c_properties(path: str, target: str) -> NoReturn:
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


target = 2.4
if len(sys.argv) > 0:
    target = sys.argv[1]
for root, dirs, files in os.walk("."):
    for file in files:
        if file.endswith("pom.xml"):
            file_path = os.path.join(root, file)
            print(f"Apply {target} in  {file_path}")
            c_properties(file_path, target)
            c_activation(file_path, target)
