package com.bjvca.dmp.adx.batch

object AdxDataAllMain {

  def main(args: Array[String]): Unit = {
    AdxDataBgAll.getRpt()

    AdxDataPvSuccessAll.getRpt()

    AdxDataPvFailAll.getRpt()

  }


}
