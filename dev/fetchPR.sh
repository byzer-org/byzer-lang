pr="${1}"
if [ -z "${pr}" ]
then
  echo "PR Number is required"
  exit 1
fi
git fetch origin pull/${pr}/head:pr${pr}
git co pr${1}
