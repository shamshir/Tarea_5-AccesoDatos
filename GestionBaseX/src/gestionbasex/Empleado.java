package gestionbasex;

import java.text.SimpleDateFormat;
import java.util.Date;

public class Empleado {

    private String codigo;
    private String apellido;
    private String oficio;
    private Date fechaAlta;
    private int salario;
    private int comision;
    private String codigoJefe;
    private String codigoDepartamento;

    public Empleado(String codigo, String apellido, String oficio, Date fechaAlta,
            int salario, int comision, String codigoJefe, String codigoDepartamento) {

        this.codigo = codigo;
        this.apellido = apellido;
        this.oficio = oficio;
        this.fechaAlta = fechaAlta;
        this.salario = salario;
        this.comision = comision;
        this.codigoJefe = codigoJefe;
        this.codigoDepartamento = codigoDepartamento;
    }

    public String getCodigo() {
        return codigo;
    }

    public void setCodigo(String codigo) {
        this.codigo = codigo;
    }

    public String getApellido() {
        return apellido;
    }

    public void setApellido(String apellido) {
        this.apellido = apellido;
    }

    public String getOficio() {
        return oficio;
    }

    public void setOficio(String oficio) {
        this.oficio = oficio;
    }

    public Date getFechaAlta() {
        return fechaAlta;
    }

    public void setFechaAlta(Date fechaAlta) {
        this.fechaAlta = fechaAlta;
    }

    public int getSalario() {
        return salario;
    }

    public void setSalario(int salario) {
        this.salario = salario;
    }

    public int getComision() {
        return comision;
    }

    public void setComision(int comision) {
        this.comision = comision;
    }

    public String getCodigoJefe() {
        return codigoJefe;
    }

    public void setCodigoJefe(String codigoJefe) {
        this.codigoJefe = codigoJefe;
    }

    public String getCodigoDepartamento() {
        return codigoDepartamento;
    }

    public void setCodigoDepartamento(String codigoDepartamento) {
        this.codigoDepartamento = codigoDepartamento;
    }

    public void imprimir() {

        System.out.println("Empleado/a: "
                + this.codigo + ", "
                + this.apellido + ", "
                + this.oficio + ", "
                + this.fechaAlta + ", "
                + this.salario + ", "
                + this.comision + ", "
                + this.codigoJefe + ", "
                + this.codigoDepartamento);
    }

    public String getCodigoInsercion() {

        String codigoInsercion = "insert node "
                + "<emp codi=\"" + this.codigo + "\" dept=\"" + this.codigoDepartamento + "\"";
        if (this.codigoJefe != null) {
            codigoInsercion += " cap=\"" + this.codigoJefe + "\">";
        } else {
            codigoInsercion += ">";
        }
        codigoInsercion += "<cognom>" + this.apellido + "</cognom>";
        codigoInsercion += "<ofici>" + this.oficio + "</ofici>";
        codigoInsercion += "<dataAlta>" + dateToString(this.fechaAlta) + "</dataAlta>";
        codigoInsercion += "<salari>" + this.salario + "</salari>";
        if (this.comision != 0) {
            codigoInsercion += "<comissio>" + this.comision + "</comissio>";
        }
        codigoInsercion += "</emp> as last into /empresa/empleats";

        return codigoInsercion;
    }

    private String dateToString(Date fechaSinProcesar) {

        SimpleDateFormat formatoFecha = new SimpleDateFormat("yyyy-mm-dd");
        String fechaProcesada = formatoFecha.format(fechaSinProcesar);
        return fechaProcesada;
    }

}
