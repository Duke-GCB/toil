class: CommandLineTool
description: "Write the current environment to a file"
inputs: []
outputs:
  - id: "#output"
    type: File
    outputBinding:
      glob: output.txt
baseCommand: env

# Specify that the standard output stream must be redirected to a file called
# output.txt in the designated output directory.
stdout: output.txt
