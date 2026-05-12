resource "aws_instance" "flink" {
  ami                    = data.aws_ami.ubuntu.id
  instance_type          = "c5.4xlarge"
  key_name               = var.key_pair_name
  subnet_id              = aws_subnet.public.id
  vpc_security_group_ids = [aws_security_group.pipeline.id]

  root_block_device {
    volume_size = 50
    volume_type = "gp3"
  }

  user_data = templatefile("${path.module}/userdata/flink_setup.sh", {
    kafka_broker = aws_instance.kafka.private_ip
    mongodb_uri  = var.mongodb_uri
  })

  depends_on = [aws_instance.kafka]

  tags = {
    Name = "${var.project_name}-flink"
  }
}
