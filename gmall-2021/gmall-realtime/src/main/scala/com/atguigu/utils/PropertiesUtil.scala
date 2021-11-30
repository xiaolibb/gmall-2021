package com.atguigu.utils

import java.io.InputStreamReader
import java.util.Properties

/**
 * @Author HP  
 * @Date: 2021-11-30 14:08
 * @Description:
 */
object PropertiesUtil {
  def load(propertieName: String): Properties = {
    val prop = new Properties()
    prop.load(new InputStreamReader(Thread.currentThread().getContextClassLoader.getResourceAsStream(propertieName), "UTF-8"))
    prop
  }
}
