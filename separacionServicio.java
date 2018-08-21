package migracionEnlace;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import contantes.Constantes;

import dto.TipoDTO;

public class separacionServicio {

	public static void main(String argv[]) {
		
		System.out.println("Inicio del Proceso");
		
		try {
			
			Connection conn = abreConexion("teleductos");
		
			limpiaTabla(conn);			
			
			//List <TipoDTO> serviciosDuplicados = obtieneListaServiciosDuplicados(conn);
			List <TipoDTO> lServicios = obtieneListaServicio(conn);	
			
			List<String> listaRedCliente = obtieneListaRedCliente(conn);
			
			for (TipoDTO servicio : lServicios) {
				
				switch (Integer.parseInt(servicio.getValor())) {

					case Constantes.INTERNET_EMPRESA:
						ejecutaCasoInternetEmpresa(servicio, conn);
						break;
					case Constantes.INTERNET_GRANDES_CLIENTES:
						ejecutaCasoInternetGandesClientes(servicio, conn);
						break;
					case Constantes.INTERNET_EMPRENDEDOR:
						ejecutaCasoInternetEmprendedor(servicio, conn);
						break;
					case Constantes.PLAN_DATACENTER:
						ejecutaCasoPlanDatacenter(servicio, conn);
						break;
					case Constantes.RED_IP_UTP:
						ejecutaCasoRedIpUtp(servicio, conn);	
						break;
					case Constantes.RED_IP_FO:
						ejecutaCasoRedIpFo(servicio, listaRedCliente, conn);
						break;
					case Constantes.RED_IP_CU:
						ejecutaCasoRedIpCu(servicio, listaRedCliente, conn);
						break;
						
					case Constantes.MALL_1525:
					case Constantes.MALL_1602:
					case Constantes.MALL_2202:
					case Constantes.MALL_2522:
					case Constantes.MALL_2262:
					case Constantes.MALL_2462:
					case Constantes.MALL_2043:
					case Constantes.MALL_2542:
					case Constantes.MALL_1762:
					case Constantes.MALL_2182:
					case Constantes.MALL_2183:
					case Constantes.MALL_2362:
					case Constantes.MALL_2882:
					case Constantes.MALL_2622:
					case Constantes.MALL_2762:
					case Constantes.MALL_2842:
					case Constantes.MALL_2982:
					case Constantes.MALL_2642:
					case Constantes.MALL_2822:
					case Constantes.MALL_2562:
					case Constantes.MALL_2662:
					case Constantes.MALL_2802:
					case Constantes.MALL_2922:
					case Constantes.MALL_2963:
						ejecutaCasoMall(servicio, conn);	
						break;			
						
					case Constantes.IP_TV_EMPRESAS:
						ejecutaCasoIPTVEmpresas(servicio, conn);
						break;
						
					case Constantes.INTERNET_ADVANCE:
						ejecutaCasoPlanInternetAdvanced(servicio, conn);
						break;
				}		
			}
			//cargaEquiposCaso1();		
			
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
		System.out.println("Fin del Proceso");		
	}
	
	private static void ejecutaCasoRedIpCu(TipoDTO servicio, List<String> listaRedCliente, Connection conn) throws SQLException {

//		Connection conn = abreConexion("teleductos");
		
		String servicioPrincipalRespalo = esServicioRespaldo(servicio, conn);
		//String servicioPrincipalRespalo = null;
		
		if (!esDireccionOrigenRedCliente(servicio,listaRedCliente,conn)) {
			
			if (!esEntronqueOrigen(servicio, conn)) {			
				insertaRedPrivada(null, servicio,"OK",null,"Conexion Privada","10001322",null,"ORIGEN","EnMPLSCU" + servicioPrincipalRespalo,conn);
				insertaEnlaceMplsFO(servicioPrincipalRespalo, servicio,"OK",null,"Enlace MPLS CU","10001193","47","ORIGEN",conn);
			} else {				
				insertaRedPrivada(null, servicio,"OK",null,"Conexion Privada","10001322",null,"ORIGEN",null,conn);			
			}
			
		} else {
			insertaEnlaceMplsFO(servicioPrincipalRespalo, servicio,"ERROR","ENLACE MPLS FO CON RED CLIENTE SI EN ORIGEN","Enlace MPLS CU","10001193","47","ORIGEN",conn);
		}
		
		if (!esEntronqueDestino(servicio, conn)) {
			
			if (!esDireccionDestinoRedCliente(servicio,conn)) {
				insertaRedPrivada(null, servicio,"OK",null,"Conexion Privada","10001322",null,"DESTINO","EnMPLSCU" + servicioPrincipalRespalo,conn);
				insertaEnlaceMplsFO(servicioPrincipalRespalo, servicio,"OK",null,"Enlace MPLS CU","10001193","47","DESTINO",conn);				
			} else {				
				insertaRedPrivada(null, servicio,"OK",null,"Conexion Privada","10001322",null,"DESTINO",null,conn);
			}
			
		} else {			
			insertaEnlaceMplsFO(servicioPrincipalRespalo, servicio,"ERROR","ENLACE MPLS FO CON RED CLIENTE SI EN ORIGEN","Enlace MPLS CU","10001193","47","DESTINO",conn);
		}
		
		conn.commit();
//		
//		conn.close();
//		conn = null;		
	}
	

	private static void ejecutaCasoRedIpFo(TipoDTO servicio, List<String> listaRedCliente, Connection conn) throws SQLException {

//		Connection conn = abreConexion("teleductos");
		
		String servicioPrincipalRespalo = esServicioRespaldo(servicio, conn);
		//String servicioPrincipalRespalo = null;
		
		if (!esDireccionOrigenRedCliente(servicio,listaRedCliente,conn)) {
			
			if (!esEntronqueOrigen(servicio, conn)) {
				insertaRedPrivada(null, servicio,"OK",null,"Conexion Privada","10001322",null,"ORIGEN","EnMPLSFO" + servicioPrincipalRespalo,conn);
				insertaEnlaceMplsFO(servicioPrincipalRespalo, servicio,"OK",null,"Enlace MPLS FO","10000045","46","ORIGEN",conn);
			} else {
				insertaRedPrivada(null, servicio,"OK",null,"Conexion Privada","10001322",null,"ORIGEN",null,conn);
			}
			
		} else {
			insertaEnlaceMplsFO(servicioPrincipalRespalo, servicio,"ERROR","ENLACE MPLS FO CON RED CLIENTE SI EN ORIGEN","Enlace MPLS FO","10001255","46","ORIGEN",conn);
		}
		
		if (!esEntronqueDestino(servicio, conn)) {
			
			if (!esDireccionDestinoRedCliente(servicio,conn)) {
				insertaRedPrivada(null, servicio,"OK",null,"Conexion Privada","10001322",null,"DESTINO","EnMPLSFO" + servicioPrincipalRespalo,conn);
				insertaEnlaceMplsFO(servicioPrincipalRespalo, servicio,"OK",null,"Enlace MPLS FO","10000045","46","DESTINO",conn);				
			} else {				
				insertaRedPrivada(null, servicio,"OK",null,"Conexion Privada","10001322",null,"DESTINO",null,conn);
			}
			
		} else {			
			insertaEnlaceMplsFO(servicioPrincipalRespalo, servicio,"ERROR","ENLACE MPLS FO CON RED CLIENTE SI EN ORIGEN","Enlace MPLS FO","10000045","46","DESTINO",conn);
		}
		
		conn.commit();
		
//		conn.close();
//		conn = null;
	}


	private static void insertaRedPrivada(String codServPrincipal, TipoDTO servicio, String estado, String descripcion, String producto,
			String offering, String codProducto, String origenDestino, String accessId, Connection conn) {
		
		try {
			
			String sql =
				"insert into teleductos.servicio_migracion2\n" +
				"  Select teleductos.id_migracion.nextval,s.COD_SERVICIO,\n" ;
			
				if ("ORIGEN".equals(origenDestino)) {
					sql = sql +"         'O_' || teleductos.servicio_migracion_seq.nextval servicio_migracion,\n" ;
				} else {
					sql = sql +"         'D_' || teleductos.servicio_migracion_seq.nextval servicio_migracion,\n" ;
				}
			
				sql = sql + "         ?,\n" + 
				"         ?,\n" + 
				"         s.costo_incorporacion,\n" + 
				"         S.COD_MONEDA_RENTA,\n" + 
				"         ?,\n" + 
				"         ?,\n" + 
				"         user,\n" + 
				"         sysdate,\n" + 
				"         md1.cod_direccion,\n" + 
				"         md1.direccion,\n" + 
				"         ?,\n" + 
				"         ?,\n" + 
				"         ?,\n" +
				"         ?,\n" +
				"         ?\n" + 
				"    From TELEDUCTOS.SERVICIO    s,\n" + 
				"         COMUN.PRODUCTO_DETALLE pd,\n" + 
				"         COMUN.PRODUCTO_CLIENTE pc,\n" + 
				"         COMUN.MAE_DIRECCIONES  md1\n" + 
				"   Where pc.COD_PRODUCTO = pd.COD_PRODUCTO\n" + 
				"     And pd.COD_PRODUCTO_DETALLE = s.COD_PRODUCTO_CLIENTE\n" ; 
				
				if ("ORIGEN".equals(origenDestino)) {
					sql = sql + "     And md1.COD_DIRECCION = s.COD_DIRECCION_ORIGEN\n" ;
				} else {
					sql = sql + "     And md1.COD_DIRECCION = s.COD_DIRECCION_DESTINO\n" ;	
				}
				 
				sql = sql + "     and s.cod_servicio = ?\n" + 
				"     And s.FEC_FIN_VIGENCIA > SysDate\n" + 
				"     And s.ESTADO_SERVICIO Not In ('ANULA', 'RETIRA')";

				//System.out.println(sql);
				
			PreparedStatement stmt = conn.prepareStatement(sql);
			
			stmt.setString(1, codServPrincipal);
			//System.out.println(codServPrincipal);

			stmt.setString(2, origenDestino);
			//System.out.println(origenDestino);

			stmt.setString(3, codProducto);
			//System.out.println(codProducto);

			stmt.setString(4, producto);
			//System.out.println(producto);

			stmt.setString(5, offering);		
			//System.out.println(offering);

			stmt.setString(6, estado);
			//System.out.println(estado);
			
			stmt.setString(7, descripcion);	
			//System.out.println(descripcion);
			
			
			if(codServPrincipal != null){
				stmt.setString(8, Constantes.TIPO_RELACION_RESPALDO);
			} else {
				stmt.setString(8, "");
			}
			
			stmt.setString(9, accessId);
			
			stmt.setString(10, servicio.getCodValor());
		
			stmt.executeUpdate();
			
			stmt.close();
			stmt = null;
		
		} catch (SQLException e) {
			e.printStackTrace();
		}		
	}

	private static void insertaEnlaceMplsFO(String codServPrincipal, TipoDTO servicio, String estado, String descripcion, String producto,
			String offering, String codProducto, String origenDestino, Connection conn) {

		//System.out.println("insertaEnlaceMplsFO");
		
		try {
			
			String sql =
				"insert into teleductos.servicio_migracion2\n" +
				"  Select teleductos.id_migracion.nextval,\n" + 
				"         s.COD_SERVICIO,\n" ;
			
				if ("ORIGEN".equals(origenDestino)) {
					sql = sql + "      'O_' ||  s.COD_SERVICIO,\n" ;
				} else {
					sql = sql + "      'D_' ||  s.COD_SERVICIO,\n" ;
				}
				
				sql = sql +"         ?,\n" + 
				"         ?,\n" + 
				"         s.costo_incorporacion,\n" + 
				"         S.COD_MONEDA_RENTA,\n" + 
				"         ?,\n" + 
				"         ?,\n" + 
				"         user,\n" + 
				"         sysdate,\n" + 
				"         md1.cod_direccion,\n" + 
				"         md1.direccion,\n" + 
				"         ?,\n" + 
				"         ?,\n" + 
				"         ?,\n" +
				"         ?\n" + 
				"    From TELEDUCTOS.SERVICIO    s,\n" + 
				"         COMUN.PRODUCTO_DETALLE pd,\n" + 
				"         COMUN.PRODUCTO_CLIENTE pc,\n" + 
				"         COMUN.MAE_DIRECCIONES  md1\n" + 
				"   Where pc.COD_PRODUCTO = pd.COD_PRODUCTO\n" + 
				"     And pd.COD_PRODUCTO_DETALLE = s.COD_PRODUCTO_CLIENTE\n" ; 
				
				if ("ORIGEN".equals(origenDestino)) {
					sql = sql + "     And md1.COD_DIRECCION = s.COD_DIRECCION_ORIGEN\n" ;
				} else {
					sql = sql + "     And md1.COD_DIRECCION = s.COD_DIRECCION_DESTINO\n" ;	
				}
				 
				sql = sql + "     and s.cod_servicio = ?\n" + 
				"     And s.FEC_FIN_VIGENCIA > SysDate\n" + 
				"     And s.ESTADO_SERVICIO Not In ('ANULA', 'RETIRA')";

			PreparedStatement stmt = conn.prepareStatement(sql);
			
			stmt.setString(1, codServPrincipal);
			stmt.setString(2, origenDestino);
			stmt.setString(3, codProducto);
			stmt.setString(4, producto);
			stmt.setString(5, offering);		
			stmt.setString(6, estado);
			stmt.setString(7, descripcion);		
			
			if(codServPrincipal != null){
				stmt.setString(8, Constantes.TIPO_RELACION_RESPALDO);
			} else {
				stmt.setString(8, null);
			}
			
			stmt.setString(9, servicio.getCodValor());
		
			stmt.executeUpdate();
			
			stmt.close();
			stmt = null;
		
		} catch (SQLException e) {
			e.printStackTrace();
		}		
	}

	private static String esServicioRespaldo(TipoDTO servicio, Connection conn) {
		
		String codigoServicioPrincipal = "";		
		
		
		/*
		try {
			String sql = "SELECT sr.cod_servicio_relacion " +
					"FROM teleductos.servicio_relacion sr " +
					"WHERE sr.TIPO_RELACION = 'RES' " +
					"and COD_SERVICIO = ?";
			
			PreparedStatement stmt = conn.prepareStatement(sql);
			stmt.setString(1, servicio.getCodValor());
		
			ResultSet rs = stmt.executeQuery();			
	
			if (rs.next()) {
				codigoServicioPrincipal = rs.getString("cod_servicio_relacion");
			}	
			
			rs.close();
			rs = null;
			
			stmt.close();
			stmt = null;
			
		} catch (SQLException e) {
			e.printStackTrace();
		} //codigo servicio
		*/
		return codigoServicioPrincipal;
	}

	private static boolean esDireccionOrigenRedCliente(TipoDTO servicio, List<String> lRedCliente, Connection conn) {
			boolean esRedCliente = false;
		
		
		try {
			String sql =
				"SELECT\n" +
				"  S.COD_SERVICIO\n" + 
				"FROM\n" + 
				"  TELEDUCTOS.SERVICIO S,\n" + 
				"  COMUN.MAE_DIRECCIONES MD\n" + 
				"WHERE\n" + 
				"  S.COD_SERVICIO = ?\n" + 
				"  AND MD.COD_DIRECCION = S.COD_DIRECCION_ORIGEN\n";

			if(lRedCliente.size()>0){
				sql = sql + " AND ";
				
				for(String redCliente: lRedCliente){
					sql = sql + " UPPER(MD.DIRECCION) LIKE '%" + redCliente +"%' OR";
				}
				
				sql = sql.substring(0, (sql.length()-2));
			}
			
			PreparedStatement stmt = conn.prepareStatement(sql);
			stmt.setString(1, servicio.getCodValor());
		
			ResultSet rs = stmt.executeQuery();
	
			if (rs.next()) {
					esRedCliente = true;
			}
			
			rs.close();
			rs = null;
			
			stmt.close();
			stmt = null;
			
		} catch (SQLException e) {
			e.printStackTrace();
		} //codigo servicio
		
		return esRedCliente;
	}

	private static boolean esDireccionDestinoRedCliente(TipoDTO servicio, Connection conn) {
		
		boolean esRedCliente = false;
	
	
		try {
			String sql =
				"SELECT\n" +
				"  S.COD_SERVICIO\n" + 
				"FROM\n" + 
				"  TELEDUCTOS.SERVICIO S,\n" + 
				"  COMUN.MAE_DIRECCIONES MD\n" + 
				"WHERE\n" + 
				"  S.COD_SERVICIO = ?\n" + 
				"  AND MD.COD_DIRECCION = S.COD_DIRECCION_DESTINO\n" + 
				"  AND UPPER(MD.DIRECCION) LIKE '%RED CLIENTE%'";

		
			PreparedStatement stmt = conn.prepareStatement(sql);
			stmt.setString(1, servicio.getCodValor());
		
			ResultSet rs = stmt.executeQuery();
			
	
			if (rs.next()) {
					esRedCliente = true;
			}
	
			
			rs.close();
			rs = null;
			
			stmt.close();
			stmt = null;
			
	} catch (SQLException e) {

		e.printStackTrace();
	} //codigo servicio
	
	return esRedCliente;
}

	private static boolean esEntronqueOrigen(TipoDTO servicio, Connection conn) {
		
		boolean esEntronqueOrigen = false;
		
		
		try {
			String sql ="SELECT S.ES_ENTRONQUE_DESDE ES_ENTRONQUE_DESDE FROM TELEDUCTOS.SERVICIO S WHERE S.COD_SERVICIO = ?";
			
			PreparedStatement stmt = conn.prepareStatement(sql);
			stmt.setString(1, servicio.getCodValor());
		
			ResultSet rs = stmt.executeQuery();
			
	
			if (rs.next()) {
				if ("1".equals(rs.getString("ES_ENTRONQUE_DESDE"))) {
					esEntronqueOrigen = true;
				}
			}
			
			rs.close();
			rs = null;
			
			stmt.close();
			stmt = null;
			
		} catch (SQLException e) {
			e.printStackTrace();
		} //codigo servicio
		
		return esEntronqueOrigen;
		
	}


	private static void ejecutaCasoMall(TipoDTO servicio, Connection conn) throws SQLException {

		String nombreOffering = "Plan Internet Mall";
		String offering ="20000916";
		
//		Connection conn = abreConexion("teleductos");
		
		if (!esEntronqueDestino(servicio,conn)) {			
			insertaPlanLogicoPlanMall(servicio,"OK",null,nombreOffering,offering,servicio.getValor(),"EnMPLSMall" + servicio.getCodValor(),conn);
			insertaEnlaceMplsMall(servicio,"OK",null,"Enlace MPLS Mall","10001255","1662",conn);			
		} else {			
			insertaPlanLogicoPlanMall(servicio,"ERROR","ERROR EN APERTURA",nombreOffering,offering,servicio.getValor(),"EnMPLSMall" + servicio.getCodValor(),conn);
			insertaEnlaceMplsMall(servicio,"ERROR","DESTINO ES ENTRONQUE","Enlace MPLS Mall","10001255","1662",conn);			
		}	
		
		conn.commit();
//		
//		conn.close();
//		conn = null;		
	}

	private static void insertaPlanLogicoPlanMall(TipoDTO servicio, String estado, String  descripcion, String producto,
			String offering, String codProducto, String accessId, Connection conn) {

		//System.out.println("insertaPlanLogicoPlanMall");
		
		try {
			
		String sql =
				"insert into teleductos.servicio_migracion2\n" +
				"  Select teleductos.id_migracion.nextval,\n" + 
				"         s.COD_SERVICIO,\n" + 
				"         'D_' || teleductos.servicio_migracion_seq.nextval servicio_migracion,\n" + 
				"         null,\n" + 
				"         'DESTINO',\n" + 
				"         null,\n" + 
				"         null,\n" + 
				"         ?,\n" + 
				"         ?,\n" + 
				"         user,\n" + 
				"         sysdate,\n" + 
				"         md1.COD_DIRECCION,\n" + 
				"         MD1.DIRECCION,\n" + 
				"         ? migration_id,\n" + 
				"         ?,\n" + 
				"         ?,\n" +
				"         null,\n" +
				"         ?\n" + 
				"    From TELEDUCTOS.SERVICIO    s,\n" + 
				"         COMUN.MAE_DIRECCIONES  md1\n" + 
				"   Where \n" + 
				"      md1.COD_DIRECCION = s.COD_DIRECCION_DESTINO\n" + 
				"     and s.cod_servicio = ? \n" + 
				"     And s.FEC_FIN_VIGENCIA > SysDate\n" + 
				"     And s.ESTADO_SERVICIO Not In ('ANULA', 'RETIRA')";

			PreparedStatement stmt = conn.prepareStatement(sql);

			stmt.setString(1, codProducto);
			stmt.setString(2, producto);
			stmt.setString(3, offering);
			stmt.setString(4, estado);
			stmt.setString(5, descripcion);
			stmt.setString(6, accessId);
			stmt.setString(7, servicio.getCodValor());
		
			stmt.executeUpdate();
			
			stmt.close();
			stmt = null;
			
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
		
	}


	private static void ejecutaCasoRedIpUtp(TipoDTO servicio, Connection conn) throws SQLException {
		
//		Connection conn = abreConexion("teleductos");
		
		insertaPlanLogicoMall(servicio,"OK",null,"Red Privada Mall (VLAN)","10001433",null,"EnMPLSMall" + servicio.getCodValor(),conn);
		insertaEnlaceMplsMall(servicio,"OK",null,"Enlace MPLS Mall","10001255","1662",conn);
		
		conn.commit();
		
//		conn.close();
//		conn = null;
	}
	
	

	private static void insertaPlanLogicoMall(TipoDTO servicio, String estado, String  descripcion, String producto,
			String offering, String codProducto, String accessId, Connection conn) {
		
		//System.out.println("insertaPlanLogicoMall");
		
		try {
			
		String sql =
				"insert into teleductos.servicio_migracion2\n" +
				"  Select teleductos.id_migracion.nextval,\n" + 
				"         s.COD_SERVICIO,\n" + 
				"         'D_' || teleductos.servicio_migracion_seq.nextval servicio_migracion,\n" + 
				"         null,\n" + 
				"         'DESTINO',\n" + 
				"         null,\n" + 
				"         null,\n" + 
				"         ?,\n" + 
				"         ?,\n" + 
				"         user,\n" + 
				"         sysdate,\n" + 
				"         md1.COD_DIRECCION,\n" + 
				"         MD1.DIRECCION,\n" + 
				"         ? migration_id,\n" + 
				"         ?,\n" + 
				"         ?,\n" +
				"         null,\n" +
				"         ?\n" + 
				"    From TELEDUCTOS.SERVICIO    s,\n" + 
				"         COMUN.MAE_DIRECCIONES  md1\n" + 
				"   Where \n" + 
				"      md1.COD_DIRECCION = s.COD_DIRECCION_DESTINO\n" + 
				"     and s.cod_servicio = ? \n" + 
				"     And s.FEC_FIN_VIGENCIA > SysDate\n" + 
				"     And s.ESTADO_SERVICIO Not In ('ANULA', 'RETIRA')";

			PreparedStatement stmt = conn.prepareStatement(sql);

			stmt.setString(1, codProducto);
			stmt.setString(2, producto);
			stmt.setString(3, offering);
			stmt.setString(4, estado);
			stmt.setString(5, descripcion);
			stmt.setString(6, accessId);
			stmt.setString(7, servicio.getCodValor());
		
			stmt.executeUpdate();
			
			stmt.close();
			stmt = null;
			
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
	}


	private static void insertaEnlaceMplsMall(TipoDTO servicio, String estado, String descripcion, String producto,
			String offering, String codProducto, Connection conn) {

		//System.out.println("insertaEnlaceMplsMall");
		
		try {
			
			String sql =
				"insert into teleductos.servicio_migracion2\n" +
				"  Select teleductos.id_migracion.nextval,\n" + 
				"         s.COD_SERVICIO,\n" + 
				"         s.COD_SERVICIO,\n" + 
				"         null,\n" + 
				"         'DESTINO',\n" + 
				"         s.costo_incorporacion,\n" + 
				"         S.COD_MONEDA_RENTA,\n" + 
				"         ?,\n" + 
				"         ?,\n" + 
				"         user,\n" + 
				"         sysdate,\n" + 
				"         md1.cod_direccion,\n" + 
				"         md1.direccion,\n" + 
				"         ?,\n" + 
				"         ?,\n" + 
				"         ?,\n" +
				"         null\n" + 
				"    From TELEDUCTOS.SERVICIO    s,\n" + 
				"         COMUN.PRODUCTO_DETALLE pd,\n" + 
				"         COMUN.PRODUCTO_CLIENTE pc,\n" + 
				"         COMUN.MAE_DIRECCIONES  md1\n" + 
				"   Where pc.COD_PRODUCTO = pd.COD_PRODUCTO\n" + 
				"     And pd.COD_PRODUCTO_DETALLE = s.COD_PRODUCTO_CLIENTE\n" + 
				"     And md1.COD_DIRECCION = s.COD_DIRECCION_DESTINO\n" + 
				"     and s.cod_servicio = ?\n" + 
				"     And s.FEC_FIN_VIGENCIA > SysDate\n" + 
				"     And s.ESTADO_SERVICIO Not In ('ANULA', 'RETIRA')";

			PreparedStatement stmt = conn.prepareStatement(sql);
			
			stmt.setString(1, codProducto);
			stmt.setString(2, producto);
			stmt.setString(3, offering);		
			stmt.setString(4, estado);
			stmt.setString(5, descripcion);		
			stmt.setString(6, servicio.getCodValor());
		
			stmt.executeUpdate();
			
			stmt.close();
			stmt = null;
		
		} catch (SQLException e) {
			e.printStackTrace();
		}
	
		
	}


	private static void ejecutaCasoPlanDatacenter(TipoDTO servicio, Connection conn) throws SQLException {
		
//		Connection conn = abreConexion("teleductos");
		
		if (!esEntronqueDestino(servicio,conn)) {
			
			if(obtieneMedioInternet(servicio,conn).equals("FO")) {
				insertaPlanLogico(servicio,"OK",null,"Plan Internet Datacenter","20000739","66","EnMPLSFO" + servicio.getCodValor(),conn);
				insertaEnlaceMpls(servicio,"OK",null,"Enlace MPLS FO","10000045","66","DESTINO",conn);
			} else if(obtieneMedioInternet(servicio,conn).equals("CU")) {
				insertaPlanLogico(servicio,"OK",null,"Plan Internet Datacenter","20000739","66","EnMPLSCU" + servicio.getCodValor(),conn);
				insertaEnlaceMpls(servicio,"OK",null,"Enlace MPLS CU","10002295","66","DESTINO",conn);
			} else {
				insertaPlanLogico(servicio,"ERROR","ERROR EN APERTURA","Plan Internet Datacenter","20000739","66",null,conn);
				insertaEnlaceMpls(servicio,"ERROR","MEDIO DEL PRODUCTO NO ENCONTRADO","66",null,null,"DESTINO",conn);
			}

		} else {

			if(obtieneMedioInternet(servicio,conn).equals("FO")) {
				insertaPlanLogico(servicio,"ERROR","ERROR EN APERTURA","Plan Internet Datacenter","20000739","66","EnMPLSFO" + servicio.getCodValor(),conn);
				insertaEnlaceMpls(servicio,"ERROR","DESTINO NO ES ENTRONQUE","Enlace MPLS FO","10000045","66","DESTINO",conn);
			} else if(obtieneMedioInternet(servicio,conn).equals("CU")) {
				insertaPlanLogico(servicio,"ERROR","ERROR EN APERTURA","Plan Internet Datacenter","20000739","66","EnMPLSCU" + servicio.getCodValor(),conn);
				insertaEnlaceMpls(servicio,"ERROR","DESTINO NO ES ENTRONQUE","Enlace MPLS CU","10002295","66","DESTINO",conn);
			} else {
				insertaPlanLogico(servicio,"ERROR","ERROR EN APERTURA","Plan Internet Datacenter","20000739","66",null,conn);
				insertaEnlaceMpls(servicio,"ERROR","MEDIO DEL PRODUCTO NO ENCONTRADO",null,null,"66","DESTINO",conn);
			}
		}	
		
		conn.commit();
		
//		conn.close();
//		conn = null;
	}
	
	private static void ejecutaCasoIPTVEmpresas(TipoDTO servicio, Connection conn) throws SQLException {
		
//		Connection conn = abreConexion("teleductos");
		
		if (!esEntronqueDestino(servicio,conn)) {
			
			if(obtieneMedioInternet(servicio,conn).equals("FO")) {
				insertaPlanLogico(servicio,"OK",null,"Plan Internet Datacenter","20000739","1902","EnMPLSFO" + servicio.getCodValor(),conn);
				insertaEnlaceMpls(servicio,"OK",null,"Enlace MPLS FO","10000045","1902","DESTINO",conn);
			} else if(obtieneMedioInternet(servicio,conn).equals("CU")) {
				insertaPlanLogico(servicio,"OK",null,"Plan Internet Datacenter","20000739","1902","EnMPLSCU" + servicio.getCodValor(),conn);
				insertaEnlaceMpls(servicio,"OK",null,"Enlace MPLS CU","10002295","1902","DESTINO",conn);
			} else {
				insertaPlanLogico(servicio,"ERROR","ERROR EN APERTURA","Plan Internet Datacenter","20000739","1902",null,conn);
				insertaEnlaceMpls(servicio,"ERROR","MEDIO DEL PRODUCTO NO ENCONTRADO","1902",null,null,"DESTINO",conn);
			}

		} else {

			if(obtieneMedioInternet(servicio,conn).equals("FO")) {
				insertaPlanLogico(servicio,"ERROR","ERROR EN APERTURA","Plan Internet Datacenter","20000739","1902","EnMPLSFO" + servicio.getCodValor(),conn);
				insertaEnlaceMpls(servicio,"ERROR","DESTINO NO ES ENTRONQUE","Enlace MPLS FO","10000045","1902","DESTINO",conn);
			} else if(obtieneMedioInternet(servicio,conn).equals("CU")) {
				insertaPlanLogico(servicio,"ERROR","ERROR EN APERTURA","Plan Internet Datacenter","20000739","1902","EnMPLSCU" + servicio.getCodValor(),conn);
				insertaEnlaceMpls(servicio,"ERROR","DESTINO NO ES ENTRONQUE","Enlace MPLS CU","10002295","1902","DESTINO",conn);
			} else {
				insertaPlanLogico(servicio,"ERROR","ERROR EN APERTURA","Plan Internet Datacenter","20000739","1902",null,conn);
				insertaEnlaceMpls(servicio,"ERROR","MEDIO DEL PRODUCTO NO ENCONTRADO",null,null,"1902","DESTINO",conn);
			}
		}	
		
		conn.commit();
		
//		conn.close();
//		conn = null;
	}
	
	private static void ejecutaCasoPlanInternetAdvanced(TipoDTO servicio, Connection conn) throws SQLException {
		
//		Connection conn = abreConexion("teleductos");
		
		if (!esEntronqueDestino(servicio,conn) && "FO".equals(obtieneValorMedio(servicio, conn))) {			
			insertaPlanLogico(servicio,"OK",null,"Plan Internet Datacenter","20000739","2782","EnMPLSFO" + servicio.getCodValor(),conn);			
			insertaEnlaceMpls(servicio,"OK",null,"Enlace MPLS FO","10000045","2782","DESTINO",conn);
		} else if(esEntronqueDestino(servicio,conn) && "FO".equals(obtieneValorMedio(servicio, conn))){			
			insertaPlanLogico(servicio,"OK",null,"Plan Internet Datacenter","20000739","2782","EnMPLSFO" + servicio.getCodValor(),conn);			
		} else if("GPON".equals(obtieneValorMedio(servicio, conn))){			
			insertaPlanLogico(servicio,"OK",null,"Plan Internet Datacenter","20000739","2782",null,conn);			
		} else {			
			insertaPlanLogico(servicio,"ERROR","DESTINO NO ES ENTRONQUE","Plan Internet Datacenter","20000739","2782",null,conn);
		}	
		
		conn.commit();
//		
//		conn.close();
//		conn = null;
	}

	private static void ejecutaCasoInternetEmprendedor(TipoDTO servicio, Connection conn) throws SQLException {

//		Connection conn = abreConexion("teleductos");
		
		if (!esEntronqueDestino(servicio,conn)) {
			
			if(obtieneMedioInternet(servicio,conn).equals("FO")) {
				insertaPlanLogico(servicio,"OK",null,"Plan Internet Emprendedor","20000023","63","EnMPLSFO" + servicio.getCodValor(),conn);
				insertaEnlaceMpls(servicio,"OK",null,"Enlace MPLS FO","10000045","63","DESTINO",conn);
			} else if(obtieneMedioInternet(servicio,conn).equals("CU")) {
				insertaPlanLogico(servicio,"OK",null,"Plan Internet Emprendedor","20000023","63","EnMPLSCU" + servicio.getCodValor(),conn);
				insertaEnlaceMpls(servicio,"OK",null,"Enlace MPLS CU","10002295","63","DESTINO",conn);
			} else {
				insertaPlanLogico(servicio,"ERROR","ERROR EN APERTURA","Plan Internet Emprendedor","20000023","63",null,conn);
				insertaEnlaceMpls(servicio,"ERROR","MEDIO DEL PRODUCTO NO ENCONTRADO",null,null,"63","DESTINO",conn);
			}

		} else {
			
			TipoDTO servAsociado = obtieneServicioAsociado(conn, servicio);

			if(servAsociado == null){
				insertaPlanLogico(servicio,"ERROR","ERROR EN APERTURA","Plan Internet Emprendedor","20000023","63",null,conn);
				insertaEnlaceMpls(servicio,"ERROR","DESTINO NO ES ENTRONQUE Y SERVICIO ASOCIADO NO ES RED_IP",null,null,"63","DESTINO",conn);
			} else if(servAsociado.getValor().equals("" + Constantes.RED_IP_FO)){
				insertaPlanLogico(servicio,"OK",null,"Plan Internet Emprendedor","20000023","63","EnMPLSFO" + servAsociado.getCodValor(),conn);
				insertaEnlaceMpls(servAsociado,"OK",null,"Enlace MPLS FO","10000045","63","DESTINO",conn);
			} else if(servAsociado.getValor().equals("" + Constantes.RED_IP_CU)){
				insertaPlanLogico(servicio,"OK",null,"Plan Internet Emprendedor","20000023","63","EnMPLSCU" + servAsociado.getCodValor(),conn);
				insertaEnlaceMpls(servAsociado,"OK",null,"Enlace MPLS CU","10002295","63","DESTINO",conn);
			}
		}
		
		conn.commit();
//		
//		conn.close();
//		conn = null;
	}

	private static void ejecutaCasoInternetGandesClientes(TipoDTO servicio, Connection conn) throws SQLException {

//		Connection conn = abreConexion("teleductos");
		
		if (!esEntronqueDestino(servicio,conn)) {
			
			/*Todas las velocidades nacionales del producto son mayor a 2 Mbps*/
			insertaPlanLogico(servicio,"OK",null,"Plan Internet Grandes Clientes","20000796","1542","EnMPLSFO" + servicio.getCodValor(),conn);
			insertaEnlaceMpls(servicio,"OK",null,"Enlace MPLS FO","10000045","1542","DESTINO",conn);

		} else {
			
			TipoDTO servAsociado = obtieneServicioAsociado(conn, servicio);

			if(servAsociado == null){
				insertaPlanLogico(servicio,"ERROR","ERROR EN APERTURA","Plan Internet Grandes Clientes","20000796","1542",null,conn);
				insertaEnlaceMpls(servicio,"ERROR","DESTINO NO ES ENTRONQUE Y SERVICIO ASOCIADO NO ES RED_IP",null,null,"1542","DESTINO",conn);
			} else if(servAsociado.getValor().equals("" + Constantes.RED_IP_FO)){
				insertaPlanLogico(servicio,"OK",null,"Plan Internet Grandes Clientes","20000796","1542","EnMPLSFO" + servAsociado.getCodValor(),conn);
				insertaEnlaceMpls(servAsociado,"OK",null,"Enlace MPLS FO","10000045","1542","DESTINO",conn);
			} else if(servAsociado.getValor().equals("" + Constantes.RED_IP_CU)){
				insertaPlanLogico(servicio,"OK",null,"Plan Internet Grandes Clientes","20000796","1542","EnMPLSCU" + servAsociado.getCodValor(),conn);
				insertaEnlaceMpls(servAsociado,"OK",null,"Enlace MPLS CU","10002295","1542","DESTINO",conn);
			}
		}
		
		conn.commit();
//		
//		conn.close();
//		conn = null;	
	}

	private static void ejecutaCasoInternetEmpresa(TipoDTO servicio, Connection conn) throws SQLException {

		// Enlace MPLS CU
		
//		Connection conn = abreConexion("teleductos");
		
		if (!esEntronqueDestino(servicio,conn)) {
			
			if(obtieneMedioInternet(servicio,conn).equals("FO")) {
				insertaPlanLogico(servicio,"OK",null,"Plan Internet Empresa","20000665","61","EnMPLSFO" + servicio.getCodValor(),conn);
				insertaEnlaceMpls(servicio,"OK",null,"Enlace MPLS FO","10000045","61","DESTINO",conn);
			} else if(obtieneMedioInternet(servicio,conn).equals("CU")) {
				insertaPlanLogico(servicio,"OK",null,"Plan Internet Empresa","20000665","61", "EnMPLSCU" + servicio.getCodValor(),conn);
				insertaEnlaceMpls(servicio,"OK",null,"Enlace MPLS CU","10002295","61","DESTINO",conn);
			} else {
				insertaPlanLogico(servicio,"ERROR","ERROR EN APERTURA","Plan Internet Empresa","20000665","61",null,conn);
				insertaEnlaceMpls(servicio,"ERROR","MEDIO DEL PRODUCTO NO ENCONTRADO",null,null,"61","DESTINO",conn);
			}

		} else {
			
			TipoDTO servAsociado = obtieneServicioAsociado(conn, servicio);

			if(servAsociado == null){
				insertaPlanLogico(servicio,"ERROR","ERROR EN APERTURA","Plan Internet Empresa","20000665","61",null,conn);
				insertaEnlaceMpls(servicio,"ERROR","DESTINO NO ES ENTRONQUE Y SERVICIO ASOCIADO NO ES RED_IP",null,null,"61","DESTINO",conn);
			} else if(servAsociado.getValor().equals("" + Constantes.RED_IP_FO)){
				insertaPlanLogico(servicio,"OK",null,"Plan Internet Empresa","20000665","61","EnMPLSFO" + servAsociado.getCodValor(),conn);
				insertaEnlaceMpls(servAsociado,"OK",null,"Enlace MPLS FO","10000045","61","DESTINO",conn);
			} else if(servAsociado.getValor().equals("" + Constantes.RED_IP_CU)){
				insertaPlanLogico(servicio,"OK",null,"Plan Internet Empresa","20000665","61","EnMPLSCU" + servAsociado.getCodValor(),conn);
				insertaEnlaceMpls(servAsociado,"OK",null,"Enlace MPLS CU","10002295","61","DESTINO",conn);
			}
		}	
		
		conn.commit();
//		
//		conn.close();
//		conn = null;		
	}
	
	private static void errorServicioDuplicado(TipoDTO servicio) throws SQLException {
		
		Connection conn = abreConexion("teleductos");
		
		insertaEnlaceMpls(servicio,"ERROR","SERVICIO DUPLICADO EN EL LEGACY",null,null,null,"DESTINO",conn);
		
		conn.commit();
		
		conn.close();
		conn = null;		
	}
	
	private static String obtieneMedioInternet(TipoDTO servicio, Connection conn) {
		
		String  medio = "";
				
		try {
			String sql =
					"SELECT\n" +
					"\tCV.NOMBRE MEDIO\n" + 
					"FROM\n" + 
					"\tTELEDUCTOS.SERVICIO S,\n" + 
					"\tCOMUN.PRODUCTO_DETALLE PD,\n" + 
					"\tCOMUN.PRODUCTO_VALOR PV,\n" + 
					"\tCOMUN.CARACTERISTICA_VALOR CV\n" + 
					"WHERE\n" + 
					"\tS.COD_PRODUCTO_CLIENTE = PD.COD_PRODUCTO_DETALLE\n" + 
					"\tAND PD.COD_PRODUCTO_DETALLE = PV.COD_PRODUCTO\n" + 
					"\tAND CV.COD_VALOR = PV.COD_VALOR\n" + 
					"\tAND S.COD_SERVICIO = ?\n" + 
					"\tAND S.FEC_FIN_VIGENCIA > SYSDATE\n" + 
					"\tAND CV.COD_CARACTERISTICA = 410";
			
			PreparedStatement stmt = conn.prepareStatement(sql);
			stmt.setString(1, servicio.getCodValor());
		
			ResultSet rs = stmt.executeQuery();
	
			if (rs.next()) {
				medio = rs.getString("MEDIO");
			}
			
			rs.close();
			rs = null;
			
			stmt.close();
			stmt = null;
			
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
		return medio;		
	}

	private static void insertaEnlaceMpls(TipoDTO servicio, String estado, String descripcion, String producto, String offering, String codProducto,String origenDestino,Connection conn) {

		//System.out.println("insertaEnlaceMpls");
		
		try {
			
			String sql =
				"insert into teleductos.servicio_migracion2\n" +
				"  Select teleductos.id_migracion.nextval,\n" + 
				"         s.COD_SERVICIO,\n" ;
			
				if ("ORIGEN".equals(origenDestino)) {
					sql = sql + "      'O_' ||  s.COD_SERVICIO,\n" ;
				} else {
					sql = sql + "      'D_' ||  s.COD_SERVICIO,\n" ;
				}
				
				sql = sql + "         null,\n" + 
				"         'DESTINO',\n" + 
				"         s.costo_incorporacion,\n" + 
				"         S.COD_MONEDA_RENTA,\n" + 
				"         ?,\n" + 
				"         ?,\n" + 
				"         user,\n" + 
				"         sysdate,\n" + 
				"         md1.cod_direccion,\n" + 
				"         md1.direccion,\n" + 
				"         ?,\n" + 
				"         ?,\n" + 
				"         ?,\n" +
				"         null\n" + 
				"    From TELEDUCTOS.SERVICIO    s,\n" + 
				"         COMUN.PRODUCTO_DETALLE pd,\n" + 
				"         COMUN.PRODUCTO_CLIENTE pc,\n" + 
				"         COMUN.MAE_DIRECCIONES  md1\n" + 
				"   Where pc.COD_PRODUCTO = pd.COD_PRODUCTO\n" + 
				"     And pd.COD_PRODUCTO_DETALLE = s.COD_PRODUCTO_CLIENTE\n" ;
				
				if ("ORIGEN".equals(origenDestino)) {
					sql = sql + "     And md1.COD_DIRECCION = s.COD_DIRECCION_ORIGEN\n" ;
				} else {
					sql = sql + "     And md1.COD_DIRECCION = s.COD_DIRECCION_DESTINO\n" ;
				}
				
				sql = sql + 	"     and s.cod_servicio = ?\n" + 
				"     And s.FEC_FIN_VIGENCIA > SysDate\n" + 
				"     And s.ESTADO_SERVICIO Not In ('ANULA', 'RETIRA')";

			PreparedStatement stmt = conn.prepareStatement(sql);
			
			stmt.setString(1, codProducto);
			stmt.setString(2, producto);
			stmt.setString(3, offering);		
			stmt.setString(4, estado);
			stmt.setString(5, descripcion);		
			stmt.setString(6, servicio.getCodValor());
		
			stmt.executeUpdate();
			
			stmt.close();
			stmt = null;
		
		} catch (SQLException e) {
			e.printStackTrace();
		}		
	}

	private static void insertaPlanLogico(TipoDTO servicio, String estado,String descripcion, 
								String producto, String offering, String codProducto, String accessId, 
								Connection conn) {
		
		try {
			
		String sql =
				"insert into teleductos.servicio_migracion2\n" +
				"  Select teleductos.id_migracion.nextval,\n" + 
				"         s.COD_SERVICIO,\n" + 
				"         'D_' || teleductos.servicio_migracion_seq.nextval servicio_migracion,\n" + 
				"         null,\n" + 
				"         'DESTINO',\n" + 
				"         null,\n" + 
				"         null,\n" + 
				"         ?,\n" + 
				"         ?,\n" + 
				"         user,\n" + 
				"         sysdate,\n" + 
				"         md1.COD_DIRECCION,\n" + 
				"         MD1.DIRECCION,\n" + 
				"         ? migration_id,\n" + 
				"         ?,\n" + 
				"         ?,\n" +
				"         null,\n" +
				"         ?\n" + 
				"    From TELEDUCTOS.SERVICIO    s,\n" + 
				"         COMUN.MAE_DIRECCIONES  md1\n" + 
				"   Where \n" + 
				"      md1.COD_DIRECCION = s.COD_DIRECCION_DESTINO\n" + 
				"     and s.cod_servicio = ? \n" + 
				"     And s.FEC_FIN_VIGENCIA > SysDate\n" + 
				"     And s.ESTADO_SERVICIO Not In ('ANULA', 'RETIRA')";

			PreparedStatement stmt = conn.prepareStatement(sql);

			stmt.setString(1, codProducto);
			stmt.setString(2, producto);
			stmt.setString(3, offering);
			stmt.setString(4, estado);
			stmt.setString(5, descripcion);
			stmt.setString(6, accessId);
			stmt.setString(7, servicio.getCodValor());
		
			stmt.executeUpdate();
			
			stmt.close();
			stmt = null;
			
		} catch (SQLException e) {
			e.printStackTrace();
		}	
	}

	private static boolean esEntronqueDestino(TipoDTO servicio, Connection conn) {

		boolean esEntronqueDestino = false;
		
		
		try {
			String sql ="SELECT S.ES_ENTRONQUE_HASTA ES_ENTRONQUE_HASTA FROM TELEDUCTOS.SERVICIO S WHERE S.COD_SERVICIO = ?";
			
			PreparedStatement stmt = conn.prepareStatement(sql);
			stmt.setString(1, servicio.getCodValor());
		
			ResultSet rs = stmt.executeQuery();
			
	
			if (rs.next()) {
				if ("1".equals(rs.getString("ES_ENTRONQUE_HASTA"))) {
					esEntronqueDestino = true;
				}
			}

			
			rs.close();
			rs = null;
			
			stmt.close();
			stmt = null;
			
		} catch (SQLException e) {
			e.printStackTrace();
		} //codigo servicio
		
		return esEntronqueDestino;
		
	}
	
	private static String obtieneValorMedio(TipoDTO servicio, Connection conn) {

		String medio = "";
		
		
		try {
			String sql = "SELECT cv.nombre VALOR_MEDIO " + 
				"FROM TELEDUCTOS.SERVICIO S, comun.producto_detalle pd, comun.producto_valor pv, " +  
				"comun.caracteristica_valor cv, comun.caracteristica c " + 
				"where S.ESTADO_SERVICIO NOT IN ('ANULA','RETIRA') " + 
				"AND S.FEC_FIN_VIGENCIA > SYSDATE " + 
				"and s.cod_producto_cliente = pd.cod_producto_detalle " + 
				"and pv.cod_producto = pd.cod_producto_detalle " + 
				"and c.cod_caracteristica = cv.cod_caracteristica " + 
				"and c.nombre like 'Medio' " + 
				"and cv.cod_valor = pv.cod_valor " + 
				"and pd.cod_producto = 2782 " + 
				"and s.cod_servicio = ?";
			
			PreparedStatement stmt = conn.prepareStatement(sql);
			stmt.setString(1, servicio.getCodValor());
		
			ResultSet rs = stmt.executeQuery();
			
	
			if (rs.next()) {
				medio = rs.getString("VALOR_MEDIO");
			}
			
			rs.close();
			rs = null;
			
			stmt.close();
			stmt = null;
			
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
		return medio;		
	}

	private static List<TipoDTO> obtieneListaServicio(Connection conn) {

		List <TipoDTO> lServicios = new ArrayList<TipoDTO>();
		
		try {
			String sql =
				"SELECT S.cod_servicio, pd.cod_producto\n" +
				"  FROM TELEDUCTOS.SERVICIO S, comun.PRODUCTO_DETALLE pd\n" + 
				" WHERE pd.COD_PRODUCTO_DETALLE = s.COD_PRODUCTO_CLIENTE\n" + 
				"   AND S.ESTADO_SERVICIO NOT IN ('ANULA', 'RETIRA')\n" + 
				"   AND S.FEC_FIN_VIGENCIA > SYSDATE\n" + 
				"	AND S.FEC_FIN_VIGENCIA = (SELECT MAX(S2.FEC_FIN_VIGENCIA) FROM TELEDUCTOS.SERVICIO S2 WHERE S2.COD_SERVICIO = S.COD_SERVICIO)\n" + 
				"   AND PD.COD_PRODUCTO in\n" + 
				"       (46,47,61, 66, 63, 1542, 1662, 1682, 2202, 2522, 1602, 2262, 2462, 1525, 2043,\n" + 
				"        2542, 1762, 2182, 2183, 2362, 2882, 2622, 2762, 2842, 2982, 2642, 2822,\n" + 
				"        2562, 2662, 2802, 2922, 2963)";
			
			PreparedStatement stmt = conn.prepareStatement(sql);
			ResultSet rs = stmt.executeQuery();			
	
			while (rs.next()) {
				
				TipoDTO servicio = new TipoDTO();
				
				servicio.setCodValor(rs.getString("COD_SERVICIO"));
				servicio.setValor(rs.getString("cod_producto"));

				lServicios.add(servicio);				
			}
			
			stmt.close();
			stmt = null;
			
			rs.close();
			rs = null;

		} catch (SQLException e) {
			e.printStackTrace();
		} 
		
		return lServicios;
	}
	
	private static List<TipoDTO> obtieneListaServiciosDuplicados(Connection conn) {

		List <TipoDTO> lServicios = new ArrayList<TipoDTO>();
		
		try {
			String sql =
				"SELECT S.cod_servicio\n" +
				"  FROM TELEDUCTOS.SERVICIO S, comun.PRODUCTO_DETALLE pd\n" + 
				" WHERE pd.COD_PRODUCTO_DETALLE = s.COD_PRODUCTO_CLIENTE\n" + 
				"   AND S.ESTADO_SERVICIO NOT IN ('ANULA', 'RETIRA')\n" + 
				"   AND S.FEC_FIN_VIGENCIA > SYSDATE\n" + 
				"   AND PD.COD_PRODUCTO in\n" + 
				"       (46,47,61, 66, 63, 1542, 1662, 1682, 2202, 2522, 1602, 2262, 2462, 1525, 2043,\n" + 
				"        2542, 1762, 2182, 2183, 2362, 2882, 2622, 2762, 2842, 2982, 2642, 2822,\n" + 
				"        2562, 2662, 2802, 2922, 2963)" + 
				"        GROUP BY s.cod_servicio" + 
				"        HAVING count(*) > 1";
			
			PreparedStatement stmt = conn.prepareStatement(sql);
			ResultSet rs = stmt.executeQuery();			
	
			while (rs.next()) {
				
				TipoDTO servicio = new TipoDTO();
				
				servicio.setCodValor(rs.getString("COD_SERVICIO"));

				lServicios.add(servicio);				
			}
			
			stmt.close();
			stmt = null;
			
			rs.close();
			rs = null;

		} catch (SQLException e) {
			e.printStackTrace();
		} 
		
		return lServicios;
	}
	
	private static List<String> obtieneListaRedCliente(Connection conn) {

		List <String> lRedCliente = new ArrayList<String>();
		
		try {
			String sql =
				"select tv.valor VALOR from comun.tabla_valores tv " +
				"where tv.nombre_tabla = 'DIR_RED_CLIENTE'";
			
			PreparedStatement stmt = conn.prepareStatement(sql);
			ResultSet rs = stmt.executeQuery();			
	
			while (rs.next()) {
				lRedCliente.add(rs.getString("VALOR"));				
			}
			
			stmt.close();
			stmt = null;
			
			rs.close();
			rs = null;

		} catch (SQLException e) {
			e.printStackTrace();
		} 
		
		return lRedCliente;
	}

	private static TipoDTO obtieneServicioAsociado(Connection conn, TipoDTO codServicio) {

		TipoDTO servicioAsociado = null;
		
		try {
			String sql =
				"select sr.cod_servicio_relacion, pc.cod_producto, pc.nombre " +  
				" from teleductos.servicio_relacion sr, teleductos.servicio s, " +  
				"    comun.producto_detalle pd, comun.producto_cliente pc " + 
				" where sr.cod_servicio = ? " +  
				" and sr.tipo_relacion = 'ASO' " + 
				" and sr.cod_servicio_relacion = s.cod_servicio " + 
				" AND S.ESTADO_SERVICIO NOT IN ('ANULA','RETIRA') " +  
				" AND S.FEC_FIN_VIGENCIA > SYSDATE " + 
				" and s.cod_producto_cliente = pd.cod_producto_detalle " + 
				" and pd.cod_producto = pc.cod_producto";
			
			PreparedStatement stmt = conn.prepareStatement(sql);
			stmt.setString(1, codServicio.getCodValor());
			
			ResultSet rs = stmt.executeQuery();			
	
			while (rs.next()) {
				
				int codProducto = Integer.parseInt(rs.getString("cod_producto"));
				
				if(codProducto == Constantes.RED_IP_CU){
					servicioAsociado = new TipoDTO();
					servicioAsociado.setCodValor(rs.getString("cod_servicio_relacion"));
					servicioAsociado.setValor(String.valueOf(Constantes.RED_IP_CU));
				} else if(codProducto == Constantes.RED_IP_FO) {
					servicioAsociado = new TipoDTO();
					servicioAsociado.setCodValor(rs.getString("cod_servicio_relacion"));
					servicioAsociado.setValor(String.valueOf(Constantes.RED_IP_FO));
				}
			}
			
			stmt.close();
			stmt = null;
			
			rs.close();
			rs = null;

		} catch (SQLException e) {
			e.printStackTrace();
		} 
		
		return servicioAsociado;		
	}

	private static void limpiaTabla(Connection conn) {
		
		System.out.println("Limpia Tabla");
		
		try {
			
			String sql = "delete from teleductos.servicio_migracion2" ;
						

			PreparedStatement stmt = conn.prepareStatement(sql);
			stmt.executeUpdate();
			
			stmt.close();
			stmt = null;
			
		} catch (SQLException e) {
			e.printStackTrace();
		}		
	}

	

	private static Connection abreConexion(String usuario) {
		Connection conn = null;
		try {
			
			Class.forName("oracle.jdbc.driver.OracleDriver");
			
			  conn = DriverManager.getConnection("jdbc:oracle:thin:@192.168.1.7:1531/expgtd.grupogtd.com", usuario,"kronner");// PRoduccion
			  //conn = DriverManager.getConnection("jdbc:oracle:thin:@192.168.1.10:1531/expgtd.grupogtd.com", usuario,"massu");

			  conn.setAutoCommit(false);
			
		}  catch (SQLException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
	
		return conn;
	}
	
	
}
