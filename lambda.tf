data "archive_file" "lambda-archive" {
  type        = "zip"
  source_file = "lambda/src/main.py"
  output_path = "lambda/packages/lambda_function.zip"
}

resource "aws_lambda_function" "lambda-function" {
  filename         = "lambda/packages/lambda_function.zip"
  function_name    = "layered-test"
  role             = aws_iam_role.role_for_lambda.arn
  handler          = "main.handle"
  source_code_hash = data.archive_file.lambda-archive.output_base64sha256
  runtime          = "python3.7"
  timeout          = 15
  memory_size      = 128

  layers = [aws_lambda_layer_version.python37-pandas-layer.arn]
}

resource "aws_lambda_layer_version" "python37-pandas-layer" {
  filename         = "lambda/packages/Python3-pandas.zip"
  layer_name       = "Python3-pandas"
  source_code_hash = filebase64sha256("lambda/packages/Python3-pandas.zip")

  compatible_runtimes = ["python3.6", "python3.7"]
}

