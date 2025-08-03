go build -buildmode=c-shared -o out/go.lib -gcflags="all=-l -N"  .
python cross.py
