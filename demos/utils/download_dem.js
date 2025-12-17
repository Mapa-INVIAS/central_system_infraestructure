//Extracción masiva de archivos DEM MUNDIALES
// Varios modelo DEM para que se pueda elegir el más indicado

// DEM PARA ETOPO1

// var ETOPO1DEM = ee.Image('NOAA/NGDC/ETOPO1');
// var ETOPO1= ETOPO1DEM.select('bedrock').clip (geometry);
// var elevationVis = {
//   min: -100.0,
//   max: 5000.0,
//   palette: [
//     '0602ff', '235cb1', '307ef3', '269db1', '30c8e2', '32d3ef', '3ae237',
//     'b5e22e', 'd6e21f', 'fff705', 'ffd611', 'ffb613', 'ff8b13', 'ff6e08',
//     'ff500d', 'ff0000', 'de0101', 'c21301'
//   ],
// };
// Map.addLayer(ETOPO1, elevationVis, 'Mapa DEM');
// Export.image.toDrive({
//   image: ETOPO1.select("bedrock"),
//   description: 'ETOPO1_1850m',
//   scale: 1850,
//   region: geometry});
  

//DEM PARA ASTER GED

// var ASTER100DEM = ee.Image('NASA/ASTER_GED/AG100_003');
// var ASTER100 = ASTER100DEM.select('elevation').clip (geometry);
// Export.image.toDrive({
//   image: ASTER100.select("elevation"),
//   description: 'ASTER_100m',
//   scale: 100,
//   region: geometry});


//DEM PARA GIMP (GREENLAND)

// var GIMPDEM = ee.Image('OSU/GIMP/DEM');
// var GIMP = GIMPDEM.select('elevation').clip (geometry);
// Export.image.toDrive({
//   image: GIMP.select("elevation"),
//   description: 'GIMP_GREENLAND_30m',
//   scale: 30,
//   region: geometry});


//DEM PARA CRYOSAT2 (ANTARCTICA)

// var CryoSat2DEM = ee.Image('CPOM/CryoSat2/ANTARCTICA_DEM');
// var CryoSat2 = CryoSat2DEM.select('elevation').clip (geometry);
// Export.image.toDrive({
//   image: CryoSat2.select("elevation"),
//   description: 'CryoSat2_ANTARCTIC_1000m',
//   scale: 1000,
//   region: geometry});


//DEM PARA ALOS DSM

// var ALOSDSMDEM = ee.Image  ('JAXA/ALOS/AW3D30_V1_1');
// var ALOSDSM = ALOSDSMDEM.select('AVE').clip (geometry);
// Export.image.toDrive({
//   image: ALOSDSM.select("AVE"),
//   description: 'ALOSDSM_30m',
//   scale: 30,
//   region: geometry});


// DEM PARA GMTED2010

// var GMTED2010DEM = ee.Image('USGS/GMTED2010');
// var GMTED2010 = GMTED2010DEM.select('be75').clip (geometry);
// Export.image.toDrive({
//   image: GMTED2010.select("be75"),
//   description: 'GMTED2010_225m',
//   scale: 225,
//   region: geometry});


//DEM PARA SRTM

var SRTMDEM = ee.Image('USGS/SRTMGL1_003');
var SRTM = SRTMDEM.select('elevation').clip (geometry);
Export.image.toDrive({
  image: SRTM.select("elevation"),
  description: 'SRTM_100m',
  scale: 30,
  maxPixels: 1e13,
  region: geometry});


//DEM PARA GTOPO30

// var GTOPO30DEM = ee.Image('USGS/GTOPO30');
// var GTOPO30 = GTOPO30DEM.select('elevation').clip (geometry);
// Export.image.toDrive({
//   image: GTOPO30.select("elevation"),
//   description: 'GTOPO30_900m',
//   scale: 900,
//   region: geometry});


//DEM PARA HydroSHEDS03

// var HydroSHEDSDEM = ee.Image  ('WWF/HydroSHEDS/03CONDEM');
// var HydroSHEDS = HydroSHEDSDEM.select('b1').clip (geometry);

// Export.image.toDrive({
//   image: HydroSHEDS.select("b1"),
//   description: 'HydroSHEDS_90m',
//   scale: 90,
//   region: geometry});


// DEM PARA HydroSHEDS30

// var HydroSHEDS30DEM = ee.Image  ('WWF/HydroSHEDS/30CONDEM');
// var HydroSHEDS30 = HydroSHEDS30DEM.select('b1').clip (geometry);

// Export.image.toDrive({
//   image: HydroSHEDS30.select("b1"),
//   description: 'HydroSHEDS_900m',
//   scale: 900,
//   region: geometry});


// DEM PARA HydroSHEDS15

// var HydroSHEDS15DEM = ee.Image  ('WWF/HydroSHEDS/15CONDEM');
// var HydroSHEDS15 = HydroSHEDS15DEM.select('b1').clip (geometry);

// Export.image.toDrive({
//   image: HydroSHEDS15.select("b1"),
//   description: 'HydroSHEDS_450m',
//   scale: 450,
//   region: geometry});


// Se realiza la segmentación por regiones para que lo almacene y
// los pueda leer el MAXENT


// --- REGIONES NATURALES DE COLOMBIA ---

// Cargar departamentos de Colombia desde GEE
var dptos = ee.FeatureCollection("FAO/GAUL/2015/level1")
  .filter(ee.Filter.eq('ADM0_NAME', 'Colombia'));

// Definir regiones naturales
var caribe = dptos.filter(ee.Filter.inList('ADM1_NAME', [
  'La Guajira', 'Cesar', 'Magdalena', 'Atlántico', 'Bolívar', 'Sucre', 'Córdoba'
])).geometry();

var pacifico = dptos.filter(ee.Filter.inList('ADM1_NAME', [
  'Chocó', 'Valle del Cauca', 'Cauca', 'Nariño'
])).geometry();

var andina = dptos.filter(ee.Filter.inList('ADM1_NAME', [
  'Antioquia', 'Caldas', 'Risaralda', 'Quindío', 'Tolima', 'Huila',
  'Cundinamarca', 'Boyacá', 'Santander', 'Norte de Santander'
])).geometry();

var orinoquia = dptos.filter(ee.Filter.inList('ADM1_NAME', [
  'Arauca', 'Casanare', 'Meta', 'Vichada'
])).geometry();

var amazonia = dptos.filter(ee.Filter.inList('ADM1_NAME', [
  'Caquetá', 'Putumayo', 'Amazonas', 'Guaviare', 'Guainía', 'Vaupés'
])).geometry();

// Crear FeatureCollection final
var regiones = ee.FeatureCollection([
  ee.Feature(caribe, {nombre: 'Caribe'}),
  ee.Feature(pacifico, {nombre: 'Pacífico'}),
  ee.Feature(andina, {nombre: 'Andina'}),
  ee.Feature(orinoquia, {nombre: 'Orinoquía'}),
  ee.Feature(amazonia, {nombre: 'Amazonía'})
]);

// Visualizar
Map.centerObject(regiones, 5);
Map.addLayer(regiones, {}, 'Regiones Naturales');


// Generar las imagenes por región


// --- EXPORTAR IMAGEN POR REGIÓN NATURAL ---

var SRTM = SRTMDEM.select('elevation').clip(geometry);

regiones.evaluate(function(fc) {
  fc.features.forEach(function(f, i) {

    var region = ee.Feature(f);
    var nombre = region.get('nombre');
    
    print(typeof nombre)
    
    // se requiere remover, espacios, tildes, carácteres especiales,
    // puntos, comas y semajantes
    
    var nombre_region = nombre
      .replace(/[áÁ]/g, 'a')
      .replace(/[éÉ]/g, 'e')
      .replace(/[íÍ]/g, 'i')
      .replace(/[óÓ]/g, 'o')
      .replace(/[úÚ]/g, 'u')
      .replace(/ñ/g, 'n')
      .replace(/Ñ/g, 'N')
      .replace(/\s+/g, '_');

    Export.image.toDrive({
      image: SRTM.clip(region.geometry()),
      description: 'export_' + i,
      folder: 'invias/dem_colombia',   
      region: region.geometry(),
      scale: 30,                  
      maxPixels: 1e13
    });

  });
});

