from pyathena import connect

cursor = connect(
  profile="es-dev",
  s3_staging_dir="s3://logs.l1x.be/dwh/athena-tmp/",
  region_name="eu-west-1"
).cursor()
