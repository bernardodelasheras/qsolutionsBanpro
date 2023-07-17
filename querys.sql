select * from rpaEmpleados
where rut not in (select rut from rpaEmpleadoIngresados)



SELECT idFiniquito ,rut ,fechaFiniquito ,causal ,texto ,url 
  FROM rpaFiniquitos  
  Where convert(date, substring(fechaFiniquito,1,2) + '-' + substring(fechaFiniquito,3,2) + '-' +  substring(fechaFiniquito,5,4),105) >= '2021-01-01' 
    and rut not in (select rut from rpaFiniquitoIngresados)
  order by convert(date, substring(fechaFiniquito,1,2) + '-' + substring(fechaFiniquito,3,2) + '-' +  substring(fechaFiniquito,5,4),105)