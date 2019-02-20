package gestionbasex;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import org.basex.server.ClientQuery;
import org.basex.server.ClientSession;

public class GestorDB {

    private final String dataBase = "empresa";
    private final String ip = "localhost";
    private final int port = 1984;
    private final String user = "admin";
    private final String password = "admin";
    private ClientSession sesion;

    public GestorDB() {

        conectar();
    }

    public void conectar() {

        try {
            this.sesion = new ClientSession(this.ip, this.port, this.user, this.password);
            this.sesion.execute("open " + this.dataBase);
            System.out.println("Conexión con base de datos '" + this.dataBase + "' creada con éxito");
        } catch (IOException e) {
            System.out.println("No se ha podido establecer la conexión:");
            System.out.println("---> " + e);
        }
    }

    public void desconectar() {

        try {
            if (this.sesion == null) {
                System.out.println("La conexión ya está cerrada");
            } else {
                this.sesion.execute("close");
                this.sesion.close();
                System.out.println("Conexión cerrada con éxito");
            }
        } catch (IOException e) {
            System.out.println("No se ha podido cerrar la conexión:");
            System.out.println("---> " + e);
        }
    }

    public Departamento getDepartamentoSolo(String codigo) {

        try {
            String nombre = getDatoDepartamento(codigo, "nom");
            String localidad = getDatoDepartamento(codigo, "localitat");
            if (nombre == null) {
                throw new Exception();
            }
            Departamento departamento = new Departamento(codigo, nombre, localidad);
            return departamento;
        } catch (Exception e) {
            System.out.println("No se ha podido recuperar el departamento:");
            System.out.println("---> " + e);
            return null;
        }
    }

    public Departamento getDepartamentoCompleto(String codigo) {

        Departamento departamento = getDepartamentoSolo(codigo);
        int numEmpleados = getNumEmpleados(codigo);
        ArrayList<Empleado> listaEmpleados = new ArrayList();
        for (int i = 1; i <= numEmpleados; i++) {
            Empleado empleado = getEmpleadoSegunPosicion(codigo, i);
            listaEmpleados.add(empleado);
        }
        departamento.setEmpleados(listaEmpleados);
        return departamento;
    }

    public void insertDepartamento(Departamento departamento) {

        try {
            if (departamentoYaExistente(departamento)) {
                throw new Exception();
            }

            String lineaInsercion = departamento.getCodigoInsercion();

            if (departamento.getEmpleados() != null) {
                ArrayList<Empleado> listaEmpleados = departamento.getEmpleados();
                for (Empleado empleado : listaEmpleados) {
                    lineaInsercion += ", " + empleado.getCodigoInsercion();
                }
            }

            ClientQuery insercion = this.sesion.query(lineaInsercion);
            insercion.execute();
            insercion.close();
            System.out.println("Departamento insertado con éxito");
        } catch (Exception e) {
            System.out.println("Error al insertar el departamento:");
            System.out.println("---> " + e);
        }
    }

    public void deleteDepartamento(Departamento departamento, Departamento departamentoNuevo) {

        try {
            if (!departamentoYaExistente(departamento)) {
                throw new Exception();
            }
            String codigo = departamento.getCodigo();

            String linea = "let $dept:=/empresa/departaments/dept[@codi=\"" + codigo + "\"] "
                    + "return ("
                    + "delete node $dept, ";

            if (departamentoNuevo != null) {
                if (!departamentoYaExistente(departamentoNuevo)) {
                    throw new Exception();
                }
                String codigoNuevo = departamentoNuevo.getCodigo();
                linea += "for $emp in /empresa/empleats/emp[@dept=\"" + codigo + "\"]/@dept "
                        + "return replace value of node $emp with \"" + codigoNuevo + "\")";
            } else {
                linea += "for $emp in /empresa/empleats/emp[@dept=\"" + codigo + "\"] "
                        + "return delete node $emp)";
            }

            ClientQuery insercion = this.sesion.query(linea);
            insercion.execute();
            insercion.close();
            System.out.println("Departamento eliminado con éxito");
        } catch (Exception e) {
            System.out.println("Error al eliminar el departamento:");
            System.out.println("---> " + e);
        }
    }

    public void replaceDepartamento(Departamento dExistente, Departamento dNuevo) {

        insertDepartamento(dNuevo);

        deleteDepartamento(dExistente, dNuevo);
    }

    private String getDatoDepartamento(String codigo, String dato) {

        try {
            String lineaConsulta = "/empresa/departaments/dept[@codi=\"" + codigo + "\"]/" + dato + "/text()";
            ClientQuery consulta = this.sesion.query(lineaConsulta);
            String datoObtenido = consulta.execute();
            consulta.close();
            if (datoObtenido.equals("")) {
                throw new Exception();
            }
            return datoObtenido;
        } catch (Exception e) {
            return null;
        }
    }

    private Empleado getEmpleadoSegunPosicion(String codigoDep, int pos) {

        String codigo = getAtributoEmpleado(codigoDep, pos, "codi");
        String apellido = getDatoEmpleado(codigoDep, pos, "cognom");
        String oficio = getDatoEmpleado(codigoDep, pos, "ofici");
        Date fechaAlta = stringToDate(getDatoEmpleado(codigoDep, pos, "dataAlta"));
        int salario = stringToInt(getDatoEmpleado(codigoDep, pos, "salari"));
        int comision = stringToInt(getDatoEmpleado(codigoDep, pos, "comissio"));
        String codigoJefe = getAtributoEmpleado(codigoDep, pos, "cap");

        Empleado empleado = new Empleado(codigo, apellido, oficio, fechaAlta, salario, comision, codigoJefe, codigoDep);
        return empleado;
    }

    private int getNumEmpleados(String codigoDep) {

        try {
            String lineaConsulta = "/empresa/empleats/count(emp[@dept=\"" + codigoDep + "\"])";
            ClientQuery consulta = this.sesion.query(lineaConsulta);
            int numEmpleados = Integer.parseInt(consulta.execute());
            consulta.close();
            return numEmpleados;
        } catch (Exception e) {
            System.out.println("Error al recuperar el número de empleados:");
            System.out.println("---> " + e);
            return 0;
        }
    }

    private String getAtributoEmpleado(String codigoDep, int pos, String atributo) {

        try {
            String lineaConsulta = "/empresa/empleats/emp[@dept=\"" + codigoDep + "\"][" + pos + "]/@" + atributo + "/string()";
            ClientQuery consulta = this.sesion.query(lineaConsulta);
            String atributoObtenido = consulta.execute();
            consulta.close();
            return atributoObtenido;
        } catch (Exception e) {
            System.out.println("Error al obtener atributo de empleado:");
            System.out.println("---> " + e);
            return null;
        }
    }

    private String getDatoEmpleado(String codigoDep, int pos, String dato) {

        try {
            String lineaConsulta = "/empresa/empleats/emp[@dept=\"" + codigoDep + "\"][" + pos + "]/" + dato + "/text()";
            ClientQuery consulta = this.sesion.query(lineaConsulta);
            String datoObtenido = consulta.execute();
            consulta.close();
            return datoObtenido;
        } catch (Exception e) {
            System.out.println("Error al obtener dato de empleado:");
            System.out.println("---> " + e);
            return null;
        }
    }

    private Date stringToDate(String fechaSinProcesar) {

        try {
            SimpleDateFormat formatoFecha = new SimpleDateFormat("yyyy-mm-dd");
            Date fechaProcesada = formatoFecha.parse(fechaSinProcesar);
            return fechaProcesada;
        } catch (ParseException e) {
            System.out.println("Error al convertir la fecha:");
            System.out.println("---> " + e);
            return null;
        }
    }

    private int stringToInt(String numeroSinProcesar) {

        int numeroProcesado = 0;
        if (!numeroSinProcesar.equals("")) {
            numeroProcesado = Integer.parseInt(numeroSinProcesar);
        }
        return numeroProcesado;
    }

    private boolean departamentoYaExistente(Departamento departamentoCandidato) {

        String codigoCandidato = departamentoCandidato.getCodigo();
        if (getDatoDepartamento(codigoCandidato, "nom") != null) {
            return true;
        } else {
            return false;
        }
    }

}
