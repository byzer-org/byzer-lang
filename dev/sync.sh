git co .
git pull origin TRY  --no-edit
 ./dev/change-scala-version.sh 2.12
python ./dev/python/convert_pom.py