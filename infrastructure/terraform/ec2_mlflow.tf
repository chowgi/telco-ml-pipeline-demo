resource "aws_instance" "mlflow" {
  ami                    = data.aws_ami.ubuntu.id
  instance_type          = "t3.large"
  key_name               = var.key_pair_name
  subnet_id              = aws_subnet.public.id
  vpc_security_group_ids = [aws_security_group.mlflow.id]

  root_block_device {
    volume_size = 30
    volume_type = "gp3"
  }

  user_data = templatefile("${path.module}/userdata/mlflow_setup.sh", {
    mongodb_uri = var.mongodb_uri
  })

  tags = {
    Name = "${var.project_name}-mlflow"
  }
}
