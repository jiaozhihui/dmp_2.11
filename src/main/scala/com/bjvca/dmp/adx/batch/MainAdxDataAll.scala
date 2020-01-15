package com.bjvca.dmp.adx.batch

object MainAdxDataAll {

  def main(args: Array[String]): Unit = {
    AdxDataBgAll.getRpt()

    AdxDataPvSuccessAll.getRpt()

    AdxDataPvFailAll.getRpt()

  }


}
