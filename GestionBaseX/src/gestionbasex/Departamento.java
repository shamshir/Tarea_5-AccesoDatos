package gestionbasex;

import java.util.ArrayList;

public class Departamento {

    private String codigo;
    private String nombre;
    private String localidad;
    private ArrayList<Empleado> empleados;

    public Departamento(String codigo, String nombre, String localidad) {

        this.codigo = codigo;
        this.nombre = nombre;
        this.localidad = localidad;
    }

    public Departamento(String codigo, String nombre, String localidad, ArrayList<Empleado> empleados) {

        this.codigo = codigo;
        this.nombre = nombre;
        this.localidad = localidad;
        this.empleados = empleados;
    }

    public String getCodigo() {
        return this.codigo;
    }

    public void setCodigo(String codigo) {
        this.codigo = codigo;
    }

    public String getNombre() {
        return this.nombre;
    }

    public void setNombre(String nombre) {
        this.nombre = nombre;
    }

    public String getLocalidad() {
        return this.localidad;
    }

    public void setLocalidad(String localidad) {
        this.localidad = localidad;
    }

    public void addEmpleado(Empleado empleado) {
        if (this.empleados == null) {
            this.empleados = new ArrayList();
        }
        this.empleados.add(empleado);
    }

    public ArrayList<Empleado> getEmpleados() {
        return this.empleados;
    }

    public void setEmpleados(ArrayList<Empleado> empleados) {
        this.empleados = empleados;
    }

    public void imprimir() {

        System.out.println();
        System.out.println("Departamento: " + this.codigo + ", " + this.nombre + ", " + this.localidad);
        if (this.empleados != null) {
            System.out.println("---- Empleados ----");
            for (Empleado empleado : this.empleados) {
                empleado.imprimir();
            }
        }
        System.out.println();
    }

    public String getCodigoInsercion() {

        String codigoInsercion = "insert node "
                + "<dept codi=\"" + this.codigo + "\">"
                + "<nom>" + this.nombre + "</nom>";
        if (this.localidad != null) {
            codigoInsercion += "<localitat>" + this.localidad + "</localitat>";
        }
        codigoInsercion += "</dept> as last into /empresa/departaments";

        return codigoInsercion;
    }

}
