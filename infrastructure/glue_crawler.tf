resource "aws_glue_catalog_database" "stream" {
  name = "desafio_mod1_db"
}

resource "aws_glue_crawler" "stream" {
  database_name = aws_glue_catalog_database.stream.name
  name          = "igti_rais_processing_crawler"
  role          = aws_iam_role.glue_role.arn

  s3_target {
    path = "s3://dl-ib-igti-edc-des-mod1-tf/staging/"
  }

  tags = {
    IES   = "IGTI",
    CURSO = "EDC"
  }
}