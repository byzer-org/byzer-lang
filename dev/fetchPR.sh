pr="${1}"
if [ -z "${pr}" ]
then
  echo "PR Number is required"
  exit 1
fi
git co master
git rev-parse --verify pr${pr}
if [ $? -eq 0 ]
then
   git branch -D pr${1}
fi
git fetch origin pull/${pr}/head:pr${pr}
git co pr${1}
