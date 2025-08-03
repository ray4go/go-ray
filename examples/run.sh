SCRIPT_PATH="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

go build -buildmode=c-shared -o ${SCRIPT_PATH}/../out/raytask -gcflags="all=-l -N"  .
cd ${SCRIPT_PATH}/..
python ${SCRIPT_PATH}/app.py