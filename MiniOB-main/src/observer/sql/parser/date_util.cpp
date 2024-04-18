#include <stdio.h>
#include <string>
#include <cstring>
#include "date_util.h"
#include "sql/operator/aggregation_logical_operator.h"

bool date_flag = true;

int date_stoi(const char* str){

    int days[2][13] = { { 0,31,59,90,120,151,181,212,243,273,304,334,365 },
					{ 0,31,60,91,121,152,182,213,244,274,305,335,366} };
      int yy = -1,mm = -1,dd = -1;
      int rc = sscanf(str,"%d-%d-%d",&yy,&mm,&dd);
      if(rc != 3)
        return -1;
      int flag = 1;
      if((yy == -1 || mm == -1 || dd == -1))
        return -1;

      if(yy < 1970 || mm < 1 || dd < 1 || mm > 12)
        return -2;

      if(yy > 2038|| (yy == 2038 && mm >= 2))
        return -2;
      
      if(dd > (days[(yy%4 == 0)][mm]-days[(yy%4 == 0)][mm-1]))
        return -2;
      
      int day = 0;
      int t = 1970;
      while(t < yy){
        day += 365 + (t%4 == 0);
        t++;
      }

      day += days[(yy%4 == 0)][mm-1];
      day += dd-1;
      if(day < 0)
        flag = 0;

      if(flag){
       return day;
      } else {
        return -2;
      }
}


std::string date_itos(int day){
  int days[2][13] = { { 0,31,59,90,120,151,181,212,243,273,304,334,365 },
					{ 0,31,60,91,121,152,182,213,244,274,305,335,366} };
  int yy = 1970,mm = 1,dd = 1;
  while(day >= 365+((yy%4)==0)){
    day-= 365+(yy%4 == 0) ;
    yy++;
  }
  while(day >= days[(yy%4)==0][mm]){
    mm++;
  }
  day -= days[(yy%4)==0][mm-1];
  dd += day;
  char out[30];
  sprintf(out,"%d-%02d-%02d",yy,mm,dd);
  return std::string(out);
}