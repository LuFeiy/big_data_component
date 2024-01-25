package com.atguigu.spark.streaming

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver

import java.io.{BufferedReader, InputStreamReader}
import java.net.{ConnectException, Socket}
import java.nio.charset.StandardCharsets


class MyReceiver(host: String, port: Int) extends Receiver[String](StorageLevel.MEMORY_ONLY) {
  //创建一个Socket
  private var socket: Socket = _

  //最初启动的时候，调用该方法，作用为：读数据并将数据发送给Spark
  override def onStart(): Unit = {
    new Thread("Socket Receiver") {
      setDaemon(true)

      override def run() {
        receive()
      }
    }.start()
  }

  //读数据并将数据发送给Spark
  def receive(): Unit = {
    try {
      socket = new Socket(host, port)
      //创建一个BufferedReader用于读取端口传来的数据
      val reader = new BufferedReader(
        new InputStreamReader(
          socket.getInputStream, StandardCharsets.UTF_8))
      //定义一个变量，用来接收端口传过来的数据
      var input: String = null

      //读取数据 循环发送数据给Spark 一般要想结束发送特定的数据 如："==END=="
      while ((input = reader.readLine()) != null) {
        store(input)
      }
    } catch {
      case e: ConnectException =>
        restart(s"Error connecting to $host:$port", e)
        return
    }
  }

  override def onStop(): Unit = {
    if (socket != null) {
      socket.close()
      socket = null
    }
  }
}
