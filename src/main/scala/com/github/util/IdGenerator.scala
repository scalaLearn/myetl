package com.github.util

import java.util.UUID

/**
  * @author : ls
  * @version : Created in 下午3:14 2018/6/21
  *
  */
object IdGenerator {

  def generateShopID: String = {
    s"SHOP${UUID.randomUUID().toString.replaceAll("-", "")}"
  }

  def generateCouponID: String = {
    s"COPN${UUID.randomUUID().toString.replaceAll("-", "")}"
  }

  def generateGoodsID: String = {
    s"GOOD${UUID.randomUUID().toString.replaceAll("-", "")}"
  }

  def generateCommentID: String = {
    s"CMMT${UUID.randomUUID().toString.replaceAll("-", "")}"
  }

  def generateBrandID: String = {
    s"BRND${UUID.randomUUID().toString.replaceAll("-", "")}"
  }

  def generateAreaID: String = {
    s"AREA${UUID.randomUUID().toString.replaceAll("-", "")}"
  }

  def main(args: Array[String]) {
    val x = generateBrandID
    println(x)
  }
}
