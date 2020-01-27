package com.baidu.spark.rqg.ast

import com.baidu.spark.rqg.RQGTable

case class QuerySession(tables: Array[RQGTable])
