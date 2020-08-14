echo sync
git co .
git pull origin TRY  --no-edit  -X theirs
 ./dev/change-scala-version.sh 2.12
python ./dev/python/convert_pom.py