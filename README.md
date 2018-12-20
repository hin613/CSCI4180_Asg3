# CSCI4180_Asg3
Without handling zero chunks (marks: 84/100)

### How to run
1. run `make`
2. choose your actions
* `$ java MyDedup upload <min_chunk> <avg_chunk> <max_chunk> <d> <file_to_upload> <local|azure>`
* `$ java MyDedup download <file_to_download> <local_file_name> <local|azure>`
* `$ java MyDedup delete <file_to_delete> <local|azure>`
3. run `make clean` if you want to clear the data
