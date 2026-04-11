# ADTEC Dashboard con Autenticación

## Descripción
Dashboard de ADTEC con sistema de autenticación de usuarios.

## Instalación

1. Instalar dependencias:
```bash
npm install
pip install -r requirements.txt
```

2. Configurar variables de entorno:
```bash
cp .env.example .env
# Editar .env con tus configuraciones
```

3. Configurar usuarios:
- Edita `users_database.json` para agregar/modificar usuarios
- Los usuarios tienen roles: `admin` (acceso completo) o `visualizer` (solo lectura)

## Ejecución

```bash
npm start
```

El servidor se iniciará en `http://localhost:8000`

## Flujo de Autenticación

1. Accede a `http://localhost:8000/login`
2. Ingresa tus credenciales
3. Si son correctas, serás redirigido al dashboard
4. La sesión expira después de 15 minutos de inactividad
5. Puedes cerrar sesión manualmente con el botón "Cerrar Sesión"

## Roles y Permisos

### Admin
- Control total de actuadores
- Descarga de archivos XLSX
- Acceso a IA Assistant

### Visualizador
- Solo vista de datos
- Sin control de actuadores
- Sin descarga de archivos
- Sin acceso a IA Assistant

## Estructura de Archivos

- `login.html` - Página de inicio de sesión
- `index.html` - Dashboard principal
- `server.js` - Servidor backend con FastAPI
- `users_database.json` - Base de datos de usuarios
- `.env` - Variables de entorno

## Seguridad

- Las contraseñas están en texto plano solo para desarrollo
- En producción, hashea las contraseñas con bcrypt
- Cambia el JWT_SECRET en producción
- Usa HTTPS en producción

## Soporte

Para soporte técnico, contacta al equipo de ADTEC.
