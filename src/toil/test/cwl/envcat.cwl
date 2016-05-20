class: Workflow
description: "Prints out the current environment"
cwlVersion: "cwl:draft-3"
inputs: []
outputs:
  - id: "#output"
    type: File
    source: "#env.output"
steps:
  - id: "#env"
    inputs: []
    outputs:
      - { id: "#env.output" }
    run: { "$import": envtool.cwl }
