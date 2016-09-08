package com.bakery.woople.test

/**
  * Created by peng on 16/9/2.
  */
import akka.actor.Actor
import akka.actor.ActorSystem
import akka.actor.Props

//class HelloActor extends Actor {
//  def receive = {
//    case "hello" => println("hello back at you")
//    case _       => println("huh?")
//  }
//}
//
//object Main extends App {
//  val system = ActorSystem("HelloSystem")
//  // default Actor constructor
//  val helloActor = system.actorOf(Props[HelloActor], name = "helloactor")
//  helloActor ! "hello"
//  helloActor ! "buenos dias"
//}