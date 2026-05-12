resource "aws_instance" "kafka" {
  ami                    = data.aws_ami.ubuntu.id
  instance_type          = "t3.xlarge"
  key_name               = var.key_pair_name
  subnet_id              = aws_subnet.public.id
  vpc_security_group_ids = [aws_security_group.kafka.id]

  root_block_device {
    volume_size = 100
    volume_type = "gp3"
  }

  user_data = file("${path.module}/userdata/kafka_setup.sh")

  tags = {
    Name = "${var.project_name}-kafka"
  }
}
