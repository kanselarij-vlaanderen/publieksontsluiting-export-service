import { query, update, sparqlEscapeString, sparqlEscapeUri, sparqlEscapeInt } from 'mu';
import { queryKaleidos } from './lib/kaleidos';
import { parseResult, copyToLocalGraph } from './lib/query-helpers';

const kanselarijGraph = "http://mu.semte.ch/graphs/organizations/kanselarij";
const publicGraph = "http://mu.semte.ch/graphs/public";
const publicAccessLevel = 'http://kanselarij.vo.data.gift/id/concept/toegangs-niveaus/6ca49d86-d40f-46c9-bde3-a322aa7e5c8e';

async function copySession(uri, graph) {
  await copyToLocalGraph(`
    PREFIX besluit: <http://data.vlaanderen.be/ns/besluit#>
    PREFIX mu: <http://mu.semte.ch/vocabularies/core/>

    CONSTRUCT {
      ${sparqlEscapeUri(uri)} a besluit:Zitting ;
        mu:uuid ?uuid;
        besluit:geplandeStart ?geplandeStart .
   }
    WHERE {
      GRAPH ${sparqlEscapeUri(kanselarijGraph)} {
         ${sparqlEscapeUri(uri)} a besluit:Zitting ;
          mu:uuid ?uuid ;
          besluit:geplandeStart ?geplandeStart .
      }
    }
  `, graph);

  // Value of ext:aard is currently a string instead of a URI in Kaleidos. Valvas expects a URI.
  // Workaround with the BIND(IRI(...)) construction.
  await copyToLocalGraph(`
    PREFIX besluit: <http://data.vlaanderen.be/ns/besluit#>
    PREFIX mu: <http://mu.semte.ch/vocabularies/core/>
    PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>

    CONSTRUCT {
      ${sparqlEscapeUri(uri)} ext:aard ?type .
   }
    WHERE {
      GRAPH ${sparqlEscapeUri(kanselarijGraph)} {
         ${sparqlEscapeUri(uri)} ext:aard ?typeStr .
         BIND(IRI(STR(?typeStr)) as ?type)
      }
    }
  `, graph);
}

async function copyNewsItemForProcedurestap(procedurestapUri, sessionUri, graph, category = "nieuws") {
  // for news items it's checked at the level of the procedure step whether it should be exported
  // via the ext:inNieuwsbrief property
  // for mededelingen it's checked at the level of the agendapunt whether it should be exported
  // via the ext:toonInKortBestek property

  await copyToLocalGraph(`
    PREFIX mu: <http://mu.semte.ch/vocabularies/core/>
    PREFIX dct: <http://purl.org/dc/terms/>
    PREFIX besluitvorming: <http://data.vlaanderen.be/ns/besluitvorming#>
    PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
    PREFIX prov: <http://www.w3.org/ns/prov#>

    CONSTRUCT {
      ?newsItem a besluitvorming:NieuwsbriefInfo ;
        mu:uuid ?uuid ;
        dct:title ?title ;
        ext:htmlInhoud ?htmlInhoud ;
        ext:newsItemCategory ${sparqlEscapeString(category)} .
      ${sparqlEscapeUri(procedurestapUri)} prov:generated ?newsItem ;
        besluitvorming:isGeagendeerdVia ?agendapunt .
      ?agendapunt ext:prioriteit ?priority .
      ${sparqlEscapeUri(sessionUri)} <http://mu.semte.ch/vocabularies/ext/publishedNieuwsbriefInfo> ?newsItem .
    }
    WHERE {
      GRAPH ${sparqlEscapeUri(kanselarijGraph)} {
        ${sparqlEscapeUri(procedurestapUri)} prov:generated ?newsItem .
        ${sparqlEscapeUri(procedurestapUri)} besluitvorming:isGeagendeerdVia ?agendapunt .
        ?agendapunt ext:prioriteit ?priority .
        ?newsItem a besluitvorming:NieuwsbriefInfo ;
          mu:uuid ?uuid ;
          dct:title ?title .

        OPTIONAL { ?newsItem ext:htmlInhoud ?htmlInhoud }
      }
    }
  `, graph);

  await copyToLocalGraph(`
    PREFIX mu: <http://mu.semte.ch/vocabularies/core/>
    PREFIX dct: <http://purl.org/dc/terms/>
    PREFIX besluitvorming: <http://data.vlaanderen.be/ns/besluitvorming#>
    PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
    PREFIX prov: <http://www.w3.org/ns/prov#>

    CONSTRUCT {
       ${sparqlEscapeUri(procedurestapUri)} besluitvorming:heeftBevoegde ?heeftBevoegde .
    }
    WHERE {
      GRAPH ${sparqlEscapeUri(kanselarijGraph)} {
        ${sparqlEscapeUri(procedurestapUri)} besluitvorming:heeftBevoegde ?heeftBevoegde .
      }
    }
  `, graph);

  await copyToLocalGraph(`
    PREFIX mu: <http://mu.semte.ch/vocabularies/core/>
    PREFIX dct: <http://purl.org/dc/terms/>
    PREFIX besluitvorming: <http://data.vlaanderen.be/ns/besluitvorming#>
    PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
    PREFIX prov: <http://www.w3.org/ns/prov#>

    CONSTRUCT {
      ?newsItem ext:themesOfSubcase ?themesOfSubcase .
    }
    WHERE {
      GRAPH ${sparqlEscapeUri(kanselarijGraph)} {
        ${sparqlEscapeUri(procedurestapUri)} prov:generated ?newsItem .
        ?newsItem ext:themesOfSubcase ?themesOfSubcase .
      }
    }
  `, graph);
}

async function copyNewsItemForAgendapunt(agendapuntUri, sessionUri, graph, category = "mededeling") {
  await copyToLocalGraph(`
    PREFIX mu: <http://mu.semte.ch/vocabularies/core/>
    PREFIX dct: <http://purl.org/dc/terms/>
    PREFIX besluitvorming: <http://data.vlaanderen.be/ns/besluitvorming#>
    PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
    PREFIX prov: <http://www.w3.org/ns/prov#>
    PREFIX besluit: <http://data.vlaanderen.be/ns/besluit#>

    CONSTRUCT {
      ${sparqlEscapeUri(agendapuntUri)} prov:generated ?newsUri .
      ?newsUri a besluitvorming:NieuwsbriefInfo ;
        mu:uuid ?mededelingUuid ;
        dct:title ?title ;
        ext:htmlInhoud ?htmlInhoud ;
        ext:mededelingPrioriteit ?priority ;
        ext:newsItemCategory ${sparqlEscapeString(category)} .
      ${sparqlEscapeUri(sessionUri)} <http://mu.semte.ch/vocabularies/ext/publishedNieuwsbriefInfo> ?newsUri .
    }
    WHERE {
      GRAPH ${sparqlEscapeUri(kanselarijGraph)} {
        ${sparqlEscapeUri(agendapuntUri)} a besluit:Agendapunt ;
          mu:uuid ?agendapuntUuid ;
          ext:prioriteit ?priority .
        OPTIONAL { ${sparqlEscapeUri(agendapuntUri)} dct:title ?content }
        OPTIONAL { ${sparqlEscapeUri(agendapuntUri)} dct:alternative ?shortTitle }
        BIND(CONCAT("ap-", ?agendapuntUuid) as ?mededelingUuid)
        BIND(IRI(CONCAT("http:///kanselarij.vo.data.gift/nieuwsbrief-infos/", ?mededelingUuid)) as ?newsUri)
        BIND(COALESCE(?shortTitle, ?content) as ?title)
        BIND(COALESCE(?content, '') as ?htmlInhoud)
      }
    }
  `, graph);
}

async function copyMandateeAndPerson(mandateeUri, graph) {
  await copyToLocalGraph(`
    PREFIX mu: <http://mu.semte.ch/vocabularies/core/>
    PREFIX dct: <http://purl.org/dc/terms/>
    PREFIX mandaat: <http://data.vlaanderen.be/ns/mandaat#>
    PREFIX person: <http://www.w3.org/ns/person#>
    PREFIX foaf: <http://xmlns.com/foaf/0.1/>

    CONSTRUCT {
      ${sparqlEscapeUri(mandateeUri)} a mandaat:Mandataris ;
        mu:uuid ?uuidMandatee ;
        dct:title ?title ;
        mandaat:start ?start ;
        mandaat:rangorde ?rank ;
        mandaat:isBestuurlijkeAliasVan ?person .
      ?person a person:Person ;
        mu:uuid ?uuidPerson ;
        foaf:firstName ?firstName ;
        foaf:familyName ?familyName .
    }
    WHERE {
      GRAPH ?g {
        ${sparqlEscapeUri(mandateeUri)} a mandaat:Mandataris ;
          mu:uuid ?uuidMandatee ;
          dct:title ?title ;
          mandaat:start ?start ;
          mandaat:isBestuurlijkeAliasVan ?person .
        OPTIONAL { ${sparqlEscapeUri(mandateeUri)} mandaat:rangorde ?rank . }
        ?person mu:uuid ?uuidPerson ;
          foaf:firstName ?firstName ;
          foaf:familyName ?familyName .
      }
      VALUES ?g {
        ${sparqlEscapeUri(kanselarijGraph)}
        ${sparqlEscapeUri(publicGraph)}
      }
    }
  `, graph);

  await copyToLocalGraph(`
      PREFIX mu: <http://mu.semte.ch/vocabularies/core/>
      PREFIX dct: <http://purl.org/dc/terms/>
      PREFIX mandaat: <http://data.vlaanderen.be/ns/mandaat#>
      PREFIX person: <http://www.w3.org/ns/person#>
      PREFIX foaf: <http://xmlns.com/foaf/0.1/>
      PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>

      CONSTRUCT {
        ${sparqlEscapeUri(mandateeUri)} ?p ?o .
      }
      WHERE {
        GRAPH ?g {
          ${sparqlEscapeUri(mandateeUri)} ?p ?o .
        }
        VALUES ?g {
          ${sparqlEscapeUri(kanselarijGraph)}
          ${sparqlEscapeUri(publicGraph)}
        }
        VALUES ?p {
          mandaat:einde
          mandaat:rangorde
          ext:nickName
        }
      }
  `, graph);

  await copyToLocalGraph(`
      PREFIX mu: <http://mu.semte.ch/vocabularies/core/>
      PREFIX dct: <http://purl.org/dc/terms/>
      PREFIX mandaat: <http://data.vlaanderen.be/ns/mandaat#>
      PREFIX person: <http://www.w3.org/ns/person#>
      PREFIX foaf: <http://xmlns.com/foaf/0.1/>
      PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>

      CONSTRUCT {
        ${sparqlEscapeUri(mandateeUri)} ?p ?o .
      }
      WHERE {
        GRAPH ?g {
          ${sparqlEscapeUri(mandateeUri)} mandaat:isBestuurlijkeAliasVan ?person .
          ?person ?p ?o
        }
        VALUES ?g {
          ${sparqlEscapeUri(kanselarijGraph)}
          ${sparqlEscapeUri(publicGraph)}
        }
        VALUES ?p {
          foaf:name
        }
      }
  `, graph);
}

async function copyDocumentsForProcedurestap(procedurestapUri, graph) {
  return copyDocuments('ext:bevatDocumentversie', procedurestapUri, graph);
}

async function copyDocumentsForAgendapunt(agendapuntUri, graph) {
  return copyDocuments('ext:bevatAgendapuntDocumentversie', agendapuntUri, graph);
}

async function copyDocuments(documentVersiePredicate, resourceUri, graph) {
  await copyToLocalGraph(`
    PREFIX mu: <http://mu.semte.ch/vocabularies/core/>
    PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
    PREFIX besluitvorming: <http://data.vlaanderen.be/ns/besluitvorming#>
    PREFIX dct: <http://purl.org/dc/terms/>
    PREFIX dossier: <https://data.vlaanderen.be/ns/dossier#>
    PREFIX pav: <http://purl.org/pav/>

    CONSTRUCT {
      ${sparqlEscapeUri(resourceUri)} ext:bevatDocumentversie ?document .
      ?document a dossier:Stuk ;
        mu:uuid ?uuidDocument ;
        dct:title ?nameDocument ;
        ext:toegangsniveauVoorDocumentVersie <${publicAccessLevel}> ;
        ext:file ?file .
      ?document pav:previousVersion ?previousVersion .
      ?container a dossier:Serie ;
        dossier:collectie.bestaatUit ?document ;
        mu:uuid ?uuidContainer .
    }
    WHERE {
      GRAPH ${sparqlEscapeUri(kanselarijGraph)} {
        ${sparqlEscapeUri(resourceUri)} ${documentVersiePredicate} ?document .
        ?document a dossier:Stuk ;
          mu:uuid ?uuidDocument ;
          dct:title ?nameDocument ;
          ext:toegangsniveauVoorDocumentVersie <${publicAccessLevel}> ;
          ext:file ?file .
        OPTIONAL { ?document pav:previousVersion ?previousVersion }
        ?container a dossier:Serie ;
          dossier:collectie.bestaatUit ?document ;
          mu:uuid ?uuidContainer .
      }
    }
  `, graph);

  await copyToLocalGraph(`
    PREFIX mu: <http://mu.semte.ch/vocabularies/core/>
    PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
    PREFIX besluitvorming: <http://data.vlaanderen.be/ns/besluitvorming#>
    PREFIX dct: <http://purl.org/dc/terms/>
    PREFIX foaf: <http://xmlns.com/foaf/0.1/>
    PREFIX dossier: <https://data.vlaanderen.be/ns/dossier#>

    CONSTRUCT {
      ?container ext:documentType ?documentType .
    }
    WHERE {
      GRAPH ${sparqlEscapeUri(kanselarijGraph)} {
        ${sparqlEscapeUri(resourceUri)} ${documentVersiePredicate} ?document .
        ?container dossier:collectie.bestaatUit ?document ;
          ext:documentType ?documentType .
      }
    }
  `, graph);
}

async function copyFileTriples(documentVersionUri, graph) {
  return await copyToLocalGraph(`
    PREFIX mu: <http://mu.semte.ch/vocabularies/core/>
    PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
    PREFIX nfo: <http://www.semanticdesktop.org/ontologies/2007/03/22/nfo#>
    PREFIX nie: <http://www.semanticdesktop.org/ontologies/2007/01/19/nie#>
    PREFIX dbpedia: <http://dbpedia.org/ontology/>
    PREFIX dossier: <https://data.vlaanderen.be/ns/dossier#>

    CONSTRUCT {
      ${sparqlEscapeUri(documentVersionUri)} ext:file ?uploadFile .
      ?uploadFile a nfo:FileDataObject ;
        mu:uuid ?uuidUploadFile ;
        nfo:fileName ?fileNameUploadFile ;
        nfo:fileSize ?sizeUploadFile ;
        dbpedia:fileExtension ?extensionUploadFile .
      ?physicalFile a nfo:FileDataObject ;
        mu:uuid ?uuidPhysicalFile ;
        nfo:fileName ?fileNamePhysicalFile ;
        nfo:fileSize ?sizePhysicalFile ;
        dbpedia:fileExtension ?extensionPhysicalFile ;
        nie:dataSource ?uploadFile .
    }
    WHERE {
      GRAPH ${sparqlEscapeUri(kanselarijGraph)} {
        ${sparqlEscapeUri(documentVersionUri)} a dossier:Stuk ;
          ext:file ?uploadFile .
        ?uploadFile a nfo:FileDataObject ;
          mu:uuid ?uuidUploadFile ;
          nfo:fileName ?fileNameUploadFile ;
          nfo:fileSize ?sizeUploadFile ;
          dbpedia:fileExtension ?extensionUploadFile ;
          ^nie:dataSource ?physicalFile .
        ?physicalFile a nfo:FileDataObject ;
          mu:uuid ?uuidPhysicalFile ;
          nfo:fileName ?fileNamePhysicalFile ;
          nfo:fileSize ?sizePhysicalFile ;
          dbpedia:fileExtension ?extensionPhysicalFile .
      }
    }
  `, graph);
}

async function copyThemaCodes(graph) {
  return await copyToLocalGraph(`
    PREFIX mu: <http://mu.semte.ch/vocabularies/core/>
    PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
    PREFIX skos: <http://www.w3.org/2004/02/skos/core#>

    CONSTRUCT {
      ?s a ext:ThemaCode ;
        mu:uuid ?uuid ;
        skos:prefLabel ?label .
    }
    WHERE {
      GRAPH ${sparqlEscapeUri(publicGraph)} {
        ?s a ext:ThemaCode ;
          mu:uuid ?uuid ;
          skos:prefLabel ?label .
      }
    }
  `, graph);
}

async function copyDocumentTypes(graph) {
  return await copyToLocalGraph(`
    PREFIX mu: <http://mu.semte.ch/vocabularies/core/>
    PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
    PREFIX skos: <http://www.w3.org/2004/02/skos/core#>

    CONSTRUCT {
      ?documentType a ext:DocumentTypeCode;
        mu:uuid ?uuid ;
        skos:prefLabel ?label .
    }
    WHERE {
      GRAPH ${sparqlEscapeUri(publicGraph)} {
        ?documentType a ext:DocumentTypeCode;
          mu:uuid ?uuid ;
          skos:prefLabel ?label .
      }
    }
  `, graph);
}

async function getSession(uuid) {
  const sessions = parseResult(await queryKaleidos(`
    SELECT ?uri ?geplandeStart
    WHERE {
      GRAPH ${sparqlEscapeUri(kanselarijGraph)} {
        ?uri a <http://data.vlaanderen.be/ns/besluit#Zitting> ;
          <http://mu.semte.ch/vocabularies/core/uuid> ${sparqlEscapeString(uuid)} ;
          <http://data.vlaanderen.be/ns/besluit#geplandeStart> ?geplandeStart .
      }
    }
  `));
  return sessions.length ? sessions[0] : null;
}

async function getLatestAgendaOfSession(sessionUri) {
  const agendas = parseResult(await queryKaleidos(`
     SELECT ?uri WHERE {
       ?uri <http://data.vlaanderen.be/ns/besluit#isAangemaaktVoor> ${sparqlEscapeUri(sessionUri)} ;
          <http://mu.semte.ch/vocabularies/ext/aangemaaktOp> ?created .
     } ORDER BY DESC(?created) LIMIT 1
  `));
  return agendas.length ? agendas[0] : null;
}


async function getProcedurestappenOfAgenda(agendaUri) {
  return parseResult(await queryKaleidos(`
    PREFIX dbpedia: <http://dbpedia.org/ontology/>
    PREFIX besluitvorming: <http://data.vlaanderen.be/ns/besluitvorming#>
    PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
    PREFIX dct: <http://purl.org/dc/terms/>
    PREFIX prov: <http://www.w3.org/ns/prov#>

    SELECT ?uri ?mandatee
    WHERE {
      GRAPH ${sparqlEscapeUri(kanselarijGraph)} {
        ${sparqlEscapeUri(agendaUri)} dct:hasPart ?agendapunt .
        ?agendapunt ext:wordtGetoondAlsMededeling "false"^^<http://mu.semte.ch/vocabularies/typed-literals/boolean> ;
                    ext:prioriteit ?priorty .
        ?uri a dbpedia:UnitOfWork ;
          besluitvorming:isGeagendeerdVia ?agendapunt ;
          prov:generated ?newsItem .
        OPTIONAL { ?uri besluitvorming:heeftBevoegde ?mandatee . }
        ?newsItem a besluitvorming:NieuwsbriefInfo ;
          ext:inNieuwsbrief "true"^^<http://mu.semte.ch/vocabularies/typed-literals/boolean> .
      }
    }
  `));
}

async function getMededelingenOfAgenda(agendaUri) {
  return parseResult(await queryKaleidos(`
  PREFIX mu: <http://mu.semte.ch/vocabularies/core/>
  PREFIX dbpedia: <http://dbpedia.org/ontology/>
  PREFIX dct: <http://purl.org/dc/terms/>
  PREFIX besluitvorming: <http://data.vlaanderen.be/ns/besluitvorming#>
  PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
  PREFIX prov: <http://www.w3.org/ns/prov#>

  SELECT ?agendapunt ?priority ?procedurestap
  WHERE {
    GRAPH ${sparqlEscapeUri(kanselarijGraph)} {
      ${sparqlEscapeUri(agendaUri)} dct:hasPart ?agendapunt .
      ?agendapunt ext:wordtGetoondAlsMededeling "true"^^<http://mu.semte.ch/vocabularies/typed-literals/boolean> ;
                  ext:prioriteit ?priority ;
                  ext:toonInKortBestek  "true"^^<http://mu.semte.ch/vocabularies/typed-literals/boolean> .

      OPTIONAL {
        ?procedurestap a dbpedia:UnitOfWork ;
          mu:uuid ?uuid ;
          besluitvorming:isGeagendeerdVia ?agendapunt ;
          prov:generated ?nieuwsbriefInfo .
        ?nieuwsbriefInfo a besluitvorming:NieuwsbriefInfo .
      }
    }
  }`));
}

async function getDocumentContainers(tmpGraph) {
  return parseResult(await query(`
    PREFIX dossier: <https://data.vlaanderen.be/ns/dossier#>

    SELECT ?uri
    WHERE {
      GRAPH ${sparqlEscapeUri(tmpGraph)} {
        ?uri a dossier:Serie .
      }
    }
  `));
}

async function getLatestVersion(tmpGraph, documentUri) {
  const versions = parseResult(await query(`
    PREFIX mu: <http://mu.semte.ch/vocabularies/core/>
    PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
    PREFIX besluitvorming: <http://data.vlaanderen.be/ns/besluitvorming#>
    PREFIX dct: <http://purl.org/dc/terms/>
    PREFIX dossier: <https://data.vlaanderen.be/ns/dossier#>
    PREFIX pav: <http://purl.org/pav/>

    SELECT ?uri
    WHERE {
      GRAPH ${sparqlEscapeUri(tmpGraph)} {
        ${sparqlEscapeUri(documentUri)} a dossier:Serie ;
          dossier:collectie.bestaatUit ?uri .
        ?uri a dossier:Stuk .
        FILTER NOT EXISTS { [] pav:previousVersion ?uri }
      }
    }
    LIMIT 1
  `));

  return versions.length ? versions[0] : null;
}

async function insertDocumentAndLatestVersion(containerUri, versionUri, tmpGraph, exportGraph) {
  /*
   * Rewrites new Kaleidos model for documents to original Valvas documents model.
   * Once the Kaleidos model is consistent for new as well as legacy data, Valvas can start using the new model as well.
   */
  return await query(`
    PREFIX mu: <http://mu.semte.ch/vocabularies/core/>
    PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
    PREFIX besluitvorming: <http://data.vlaanderen.be/ns/besluitvorming#>
    PREFIX dct: <http://purl.org/dc/terms/>
    PREFIX dossier: <https://data.vlaanderen.be/ns/dossier#>

    INSERT {
      GRAPH ${sparqlEscapeUri(exportGraph)} {
        ${sparqlEscapeUri(containerUri)} a foaf:Document ;
          besluitvorming:heeftVersie ${sparqlEscapeUri(versionUri)} ;
          mu:uuid ?uuidContainer ;
          dct:title ?name ;
          ext:documentType ?documentType .
        ${sparqlEscapeUri(versionUri)} a ext:DocumentVersie ;
          mu:uuid ?uuidDocumentVersie ;
          ext:versieNummer 1 ;
          ext:file ?file .
      }
    }
    WHERE {
      GRAPH ${sparqlEscapeUri(tmpGraph)} {
        ${sparqlEscapeUri(containerUri)} a dossier:Serie ;
          dossier:collectie.bestaatUit ${sparqlEscapeUri(versionUri)} ;
          mu:uuid ?uuidContainer .
        OPTIONAL { ${sparqlEscapeUri(containerUri)} ext:documentType ?documentType . }
        ${sparqlEscapeUri(versionUri)} a dossier:Stuk ;
          mu:uuid ?uuidDocumentVersie ;
          dct:title ?name ;
          ext:file ?file .
      }
    }
  `);
}

async function linkNewsItemsToDocumentVersion(graphsWithNewsItems, tmpGraph, documentsGraph) {
  return await query(`
    PREFIX besluitvorming: <http://data.vlaanderen.be/ns/besluitvorming#>
    PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
    PREFIX prov: <http://www.w3.org/ns/prov#>

    INSERT {
      GRAPH ${sparqlEscapeUri(documentsGraph)} {
        ?newsInfo ext:documentVersie ?versie .
      }
    }
    WHERE {
      GRAPH ?g {
        ?subCaseOrAgendapunt prov:generated ?newsInfo .
        ?newsInfo a besluitvorming:NieuwsbriefInfo .
      }
      VALUES ?g {
        ${graphsWithNewsItems.map(sparqlEscapeUri).join('\n')}
      }
      GRAPH ${sparqlEscapeUri(tmpGraph)} {
        ?subCaseOrAgendapunt ext:bevatDocumentversie ?versie .
      }
    }
  `);
}

/* Takes an Array of [key, [mandatee 'a' Object, mandatee 'b' Object, ...]]-form array.
   Each array of mandatee-objects represents a group of mandatees that submitted an agendaitem.
   Assumes the mandatee array to already be sorted by priority.
   Returns an Array of [key, [mandatee 'a' Object, mandatee 'b' Object, ...]]-form arrays, which are sorted by protocol order, based on mandatee priorities
*/
function sortMandateeGroups(mandateeGroups) {
  return mandateeGroups.sort(function (a, b) {
    for (var i = 0; i < Math.max(a[1].length, b[1].length); i++) {
      if (a[1][i] && b[1][i]) {
        if (a[1][i].rank !== b[1][i].rank) {
          return a[1][i].rank - b[1][i].rank;
        } else {
          continue;
        }
      } else if (a[1][i]) {
        return 1;
      } else if (b[1][i]) {
        return -1;
      } else {
        return 0;
      }
    }
  });
}

/* Agendaitems should be grouped and ordered according to the priority of the assigned mandatee.
   Since we don't have correct historical priority data for ministers,
   we will group agendaitems per minister and let the agendaitem with the lowest
   number (priority) determine the priority of the minister.

   E.g. minister X has assigned agendaItem 3 and agendaItem 5
        minister Y has assigned agendaItem 4
   Final order of the agendaItems will be: 3 - 5 - 4
*/
async function calculatePriorityNewsItems(exportGraph) {
  let triples = [];

  // Add priority for news items with mandatee(s)

  const result = parseResult(await query(`
    PREFIX besluitvorming: <http://data.vlaanderen.be/ns/besluitvorming#>
    PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
    PREFIX mandaat: <http://data.vlaanderen.be/ns/mandaat#>
    PREFIX dct: <http://purl.org/dc/terms/>
    PREFIX prov: <http://www.w3.org/ns/prov#>

    SELECT DISTINCT ?newsItem ?number ?mandatee ?title ?rank
    WHERE {
      GRAPH ${sparqlEscapeUri(exportGraph)} {
         ?newsItem a besluitvorming:NieuwsbriefInfo .
         ?procedurestap prov:generated ?newsItem ;
             besluitvorming:heeftBevoegde ?mandatee ;
             besluitvorming:isGeagendeerdVia ?agendaItem .
         ?agendaItem ext:prioriteit ?number .
         ?mandatee dct:title ?title .
         OPTIONAL {
           ?mandatee mandaat:rangorde ?rank .
        }
      }
    }
  `));

  if (result.length > 0) {

    // [ { newsItem, number, mandatee }, ... ]

    // Group results per newsItem
    const uniqueNewsItems = {};
    result.forEach((r) => {
      const key = r.newsItem;
      uniqueNewsItems[key] = uniqueNewsItems[key] || { number: parseInt(r.number), mandatees: [] };
      if (!uniqueNewsItems[key].mandatees.some(m => r.mandatee === m.uri)) { // If mandatee not in list yet
        const mandatee = {
          uri: r.mandatee,
          title: r.title
        };
        if (r.rank !== undefined) {
          mandatee.rank = parseInt(r.rank);
        }
        // [ { uri, title, priority }, ... ]
        uniqueNewsItems[key].mandatees.push(mandatee);
      }
    });
    console.log(`Found ${Object.keys(uniqueNewsItems).length} news items`);

    // { <news-1>: { number, mandatees }, <news-2>: { number, mandatees }, ... }

    // Create 'unique' key for group of mandatees per item
    const uniqueMandateeGroups = [];
    const newsItems = [];
    for (let uri in uniqueNewsItems) {
      const item = uniqueNewsItems[uri];
      item.mandatees.sort((a, b) => a.uri - b.uri);
      const groupKey = item.mandatees.map((m) => m.uri).join();
      if (!uniqueMandateeGroups.some((group) => groupKey === group[0])) {
        uniqueMandateeGroups.push([groupKey, item.mandatees]);
      }
      newsItems.push({ uri, groupKey, number: item.number, mandatees: item.mandatees });
    }
    console.log(`Found ${uniqueMandateeGroups.length} different groups of mandatees`);
    // [ { uri: news-1, groupKey, number, mandatees, ... } ]

    let mandateePrioritiesAvailable = uniqueMandateeGroups.every((group) => group[1].every((m) => Number.isInteger(m.rank)));

    // Sort mandatee-groups
    let sortedMandateeGroups;
    console.log(`Sorting mandatee groups by ${mandateePrioritiesAvailable ? 'mandatee priority' : 'lowest agendaitem number assigned to group'}`);
    if (mandateePrioritiesAvailable) { // Based on mandatee rank
      uniqueMandateeGroups.forEach((group) => group[1].sort((a, b) => a.rank - b.rank));
      sortedMandateeGroups = sortMandateeGroups(uniqueMandateeGroups);
    } else { // Based on the lowest agendaitem number assigned to group
      sortedMandateeGroups = uniqueMandateeGroups.sort(function(a, b) {
        const lowestNumByGroupkey = (key) => Math.min(...newsItems.filter(i => i.groupKey === key).map(i => i.number));
        let na = lowestNumByGroupkey(a.groupKey);
        let nb = lowestNumByGroupkey(b.groupKey);
        return na - nb;
      });
    }
    console.log(`Sorted groups of mandatees: ${JSON.stringify(sortedMandateeGroups)}`);

    // Set overall priority per newsItem based on groupPriority and agendaItem number
    let itemPriority = 0;
    sortedMandateeGroups.forEach(function (group) {
      const groupKey = group[0];
      newsItems.filter(item => item.groupKey === groupKey)
        .sort((itemA, itemB) => itemA.number - itemB.number)
        .forEach((item) => item.priority = itemPriority++);
    });

    // Persist overall priority on newsItem in store
    triples.push(...newsItems.map( (item) => `<${item.uri}> ext:prioriteit ${sparqlEscapeInt(item.priority)} . ` ));
  }

  // Add priority for news items without mandatee

  const basePriority = 1000; // make sure they have a lower priority than the news items with a mandatee

  const resultNoMandatee = parseResult(await query(`
    PREFIX besluitvorming: <http://data.vlaanderen.be/ns/besluitvorming#>
    PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
    PREFIX mandaat: <http://data.vlaanderen.be/ns/mandaat#>
    PREFIX dct: <http://purl.org/dc/terms/>
    PREFIX prov: <http://www.w3.org/ns/prov#>

    SELECT DISTINCT ?newsItem ?number
    WHERE {
      GRAPH ${sparqlEscapeUri(exportGraph)} {
         ?newsItem a besluitvorming:NieuwsbriefInfo .
         ?procedurestap prov:generated ?newsItem ;
             besluitvorming:isGeagendeerdVia ?agendaItem .
         FILTER NOT EXISTS { ?procedurestap besluitvorming:heeftBevoegde ?mandatee . }
         ?agendaItem ext:prioriteit ?number .
      }
    }
  `));

  triples.push(...resultNoMandatee.map( (r) => `<${r.newsItem}> ext:prioriteit ${sparqlEscapeInt(basePriority + parseInt(r.number))} . ` ));

  if (triples.length) {
    await update(`
    PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>

    INSERT DATA {
      GRAPH <${exportGraph}> {
        ${triples.join('\n')}
      }
    }
   `);
  }
}

/*
  Mededeling are listed in order of the agendapunten after the news items.
*/
async function calculatePriorityMededelingen(exportGraph) {
  const results = parseResult(await query(`
    PREFIX besluitvorming: <http://data.vlaanderen.be/ns/besluitvorming#>
    PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
    PREFIX prov: <http://www.w3.org/ns/prov#>

    SELECT ?newsItem ?priority
    WHERE {
      GRAPH ${sparqlEscapeUri(exportGraph)} {
         ?newsItem a besluitvorming:NieuwsbriefInfo ;
            ext:newsItemCategory "mededeling" .
         ?procedurestap prov:generated ?newsItem ;
             besluitvorming:isGeagendeerdVia ?agendaItem .
         ?agendaItem ext:prioriteit ?priority .
      }
    }
  `));

  if (results.length == 0)
    return; // No mededelingen. No priorities to calculate.

  const basePriority = 100000; // make sure they have a lower priority than the news items

  for (let result of results) {
    const priority = basePriority + parseInt(result.priority);
    await update(`
    PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>

    INSERT DATA {
      GRAPH <${exportGraph}> {
        <${result.newsItem}> ext:prioriteit ${sparqlEscapeInt(priority)} .
      }
    }
  `);
  }
}

export {
  copySession,
  copyThemaCodes,
  copyDocumentTypes,
  copyNewsItemForProcedurestap,
  copyNewsItemForAgendapunt,
  copyMandateeAndPerson,
  copyDocumentsForProcedurestap,
  copyDocumentsForAgendapunt,
  copyFileTriples,
  getSession,
  getLatestAgendaOfSession,
  getProcedurestappenOfAgenda,
  getMededelingenOfAgenda,
  getDocumentContainers,
  getLatestVersion,
  insertDocumentAndLatestVersion,
  linkNewsItemsToDocumentVersion,
  calculatePriorityNewsItems,
  calculatePriorityMededelingen
};
