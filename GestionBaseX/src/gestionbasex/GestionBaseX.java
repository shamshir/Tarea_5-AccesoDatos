package gestionbasex;

import java.util.ArrayList;
import java.util.Date;

public class GestionBaseX {

    public static void main(String[] args) {
        GestorDB gestor = new GestorDB();

//        // Obtener Departamento sin Empleados
//        Departamento d1 = gestor.getDepartamentoSolo("d10");
//        d1.imprimir();

//        // Obtener Departamento con Empleados
//        Departamento d2 = gestor.getDepartamentoCompleto("d20");
//        d2.imprimir();

//        // Insertar Departamento
//        Departamento d3 = new Departamento("d50", "Informatica", "Palma");
//        Empleado e1 = new Empleado("e8364", "Sanchez", "Programadora", new Date(), 70000, 0, "e4785", "d50");
//        Empleado e2 = new Empleado("e4785", "Rosales", "Analista", new Date(), 100000, 5000, null, "d50");
//        Empleado e3 = new Empleado("e2189", "Mota", "Programador", new Date(), 68000, 2000, "e4785", "d50");
//        d3.addEmpleado(e1);
//        d3.addEmpleado(e2);
//        d3.addEmpleado(e3);
//        gestor.insertDepartamento(d3);

//        // Eliminar Departamento reubicando Empleados
//        Departamento d4 = gestor.getDepartamentoSolo("d10");
//        Departamento d5 = gestor.getDepartamentoSolo("d20");
//        gestor.deleteDepartamento(d4, d5);
//        d5 = gestor.getDepartamentoCompleto("d20");
//        d5.imprimir();
        
//        // Eliminar Departamento eliminando Empleados
//        Departamento d6 = gestor.getDepartamentoSolo("d20");
//        gestor.deleteDepartamento(d6, null);
        
//        // Sustituir Departamento reubicando Empleados
//        Departamento d7 = gestor.getDepartamentoCompleto("d30");
//        d7.imprimir();
//        Departamento d8 = new Departamento("d60", "Marketing", "Valencia");
//        Empleado e4 = new Empleado("e4443", "Perez", "Project Manager", new Date(), 90000, 10000, null, "d60");
//        Empleado e5 = new Empleado("e5002", "Gayà", "Diseñadora", new Date(), 80000, 4000, "e4443", "d60");
//        d8.addEmpleado(e4);
//        d8.addEmpleado(e5);
//        gestor.replaceDepartamento(d7, d8);
//        d8 = gestor.getDepartamentoCompleto("d60");
//        d8.imprimir();
        
        gestor.desconectar();
    }

}
