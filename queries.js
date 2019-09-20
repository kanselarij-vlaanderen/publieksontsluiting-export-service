import { query, update, sparqlEscapeString, sparqlEscapeUri, sparqlEscapeInt } from 'mu';
import { queryKaleidos } from './lib/kaleidos';

/**
 * Convert results of select query to an array of objects.
 * @method parseResult
 * @return {Array}
 */
function parseResult(result) {
  const bindingKeys = result.head.vars;
  return result.results.bindings.map((row) => {
    const obj = {};
    bindingKeys.forEach((key) => {
      if (row[key]) {
        obj[key] = row[key].value;
      } else {
        obj[key] = null;
      }
    });
    return obj;
  });
};

async function getMeetingUriFromKaleidos(kaleidosGraph, uuid) {
  return await queryKaleidos(`
    SELECT ?s
    WHERE {
      GRAPH ${sparqlEscapeUri(kaleidosGraph)} {
        ?s a <http://data.vlaanderen.be/ns/besluit#Zitting> ;
          <http://mu.semte.ch/vocabularies/core/uuid> ${sparqlEscapeString(uuid)} .
      }
    }
  `);
}

function constructMeetingInfo(kaleidosGraph, zitting) {
  return `
    PREFIX besluit: <http://data.vlaanderen.be/ns/besluit#>
    PREFIX mu: <http://mu.semte.ch/vocabularies/core/>

    CONSTRUCT {
      ${sparqlEscapeUri(zitting)} a besluit:Zitting ;
        mu:uuid ?uuid;
        besluit:geplandeStart ?geplandeStart .
   }
    WHERE {
      GRAPH ${sparqlEscapeUri(kaleidosGraph)} {
         ${sparqlEscapeUri(zitting)} a besluit:Zitting ;
          mu:uuid ?uuid ;
          besluit:geplandeStart ?geplandeStart .
      }
    }
  `;
}

function constructProcedurestapInfo(kaleidosGraph, meetingUri) {
  return `
  PREFIX mu: <http://mu.semte.ch/vocabularies/core/>
  PREFIX dbpedia: <http://dbpedia.org/ontology/>
  PREFIX dct: <http://purl.org/dc/terms/>
  PREFIX besluitvorming: <http://data.vlaanderen.be/ns/besluitvorming#>
  PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>

  CONSTRUCT {
    ?s a dbpedia:UnitOfWork ;
      mu:uuid ?uuid ;
      ext:wordtGetoondAlsMededeling "false"^^<http://mu.semte.ch/vocabularies/typed-literals/boolean> ;
      ext:prioriteit ?priority ;
      besluitvorming:heeftBevoegde ?heeftBevoegde .
  }
  WHERE {
    GRAPH ${sparqlEscapeUri(kaleidosGraph)} {
      ${sparqlEscapeUri(meetingUri)} besluitvorming:behandelt ?agenda .
      ?agenda dct:hasPart ?agendapunt .
      ?agendapunt ext:wordtGetoondAlsMededeling ?isMededeling ;
                  ext:prioriteit ?priorty .
      FILTER(?isMededeling = "false"^^<http://mu.semte.ch/vocabularies/typed-literals/boolean>)
      ?s a dbpedia:UnitOfWork ;
        mu:uuid ?uuid ;
        besluitvorming:isGeagendeerdVia ?agendapunt .

      OPTIONAL {
        ?s besluitvorming:heeftBevoegde ?heeftBevoegde .
      }
    }
  }`;
}

async function getProcedurestappenInfoFromTmp(tmpGraph) {
  return await query(`
    PREFIX dbpedia: <http://dbpedia.org/ontology/>
    PREFIX besluitvorming: <http://data.vlaanderen.be/ns/besluitvorming#>
    PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>

    SELECT ?s ?heeftBevoegde
    WHERE {
      GRAPH ${sparqlEscapeUri(tmpGraph)} {
        ?s a dbpedia:UnitOfWork ;
          besluitvorming:heeftBevoegde ?heeftBevoegde .
      }
    }
  `);
}

function constructNieuwsbriefInfo(kaleidosGraph, procedurestapInfo) {
  return `
    PREFIX mu: <http://mu.semte.ch/vocabularies/core/>
    PREFIX dct: <http://purl.org/dc/terms/>
    PREFIX besluitvorming: <http://data.vlaanderen.be/ns/besluitvorming#>
    PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
    PREFIX prov: <http://www.w3.org/ns/prov#>

    CONSTRUCT {
      ?s a besluitvorming:NieuwsbriefInfo ;
        mu:uuid ?uuid ;
        dct:title ?title ;
        ext:htmlInhoud ?htmlInhoud ;
        ext:themesOfSubcase ?themesOfSubcase .
      ${sparqlEscapeUri(procedurestapInfo.s)} besluitvorming:heeftBevoegde ?heeftBevoegde ;
        prov:generated ?s ;
        besluitvorming:isGeagendeerdVia ?agendapunt .
      ?agendapunt ext:prioriteit ?priorty .
    }
    WHERE {
      GRAPH ${sparqlEscapeUri(kaleidosGraph)} {
        ?s a besluitvorming:NieuwsbriefInfo ;
          ext:afgewerkt \"true\"^^<http://mu.semte.ch/vocabularies/typed-literals/boolean> ;
          ^prov:generated ${sparqlEscapeUri(procedurestapInfo.s)} ;
          mu:uuid ?uuid ;
          dct:title ?title ;
          ext:htmlInhoud ?htmlInhoud .
        OPTIONAL { ?s ext:themesOfSubcase ?themesOfSubcase .}
        OPTIONAL { ${sparqlEscapeUri(procedurestapInfo.s)} besluitvorming:heeftBevoegde ?heeftBevoegde . }
        ${sparqlEscapeUri(procedurestapInfo.s)} besluitvorming:isGeagendeerdVia ?agendapunt .
        ?agendapunt ext:prioriteit ?priorty .
      }
    }
  `;
}

async function constructLinkZittingNieuws(exportGraph, meetingUri) {
  return await query(`
    PREFIX besluitvorming: <http://data.vlaanderen.be/ns/besluitvorming#>

    INSERT {
      GRAPH ${sparqlEscapeUri(exportGraph)} {
        ${sparqlEscapeUri(meetingUri)} <http://mu.semte.ch/vocabularies/ext/publishedNieuwsbriefInfo> ?s .
      }
    }
    WHERE {
      GRAPH ${sparqlEscapeUri(exportGraph)} {
        ?s a besluitvorming:NieuwsbriefInfo .
      }
    }
  `);
}

function constructMandateeAndPersonInfo(kaleidosGraph, procedurestapInfo) {
  return `
    PREFIX mu: <http://mu.semte.ch/vocabularies/core/>
    PREFIX dct: <http://purl.org/dc/terms/>
    PREFIX mandaat: <http://data.vlaanderen.be/ns/mandaat#>
    PREFIX person: <http://www.w3.org/ns/person#>
    PREFIX foaf: <http://xmlns.com/foaf/0.1/>

    CONSTRUCT {
      ${sparqlEscapeUri(procedurestapInfo.heeftBevoegde)} a mandaat:Mandataris ;
        mu:uuid ?uuidMandatee ;
        dct:title ?title ;
        mandaat:start ?start ;
        mandaat:einde ?end ;
        mandaat:isBestuurlijkeAliasVan ?person .
      ?person a person:Person ;
        mu:uuid ?uuidPerson ;
        foaf:firstName ?firstName ;
        foaf:familyName ?familyName ;
        foaf:name ?name .
    }
    WHERE {
      GRAPH ${sparqlEscapeUri(kaleidosGraph)} {
        ${sparqlEscapeUri(procedurestapInfo.heeftBevoegde)} a mandaat:Mandataris ;
          mu:uuid ?uuidMandatee ;
          dct:title ?title ;
          mandaat:start ?start ;
          mandaat:isBestuurlijkeAliasVan ?person .
        OPTIONAL { ${sparqlEscapeUri(procedurestapInfo.heeftBevoegde)} mandaat:einde ?end . }
        ?person mu:uuid ?uuidPerson ;
          foaf:firstName ?firstName ;
          foaf:familyName ?familyName .
        OPTIONAL { ?person foaf:name ?name . }
      }
    }
  `;
}

/* Agendaitems should be grouped and ordered according to the priority of the assigned mandatee.
   Since we don't have correct historical priority data for ministers,
   we will group agendaitems per minister and let the agendaitem with the lowest
   number (priority) determine the priority of the minister.

   E.g. minister X has assigned agendaItem 3 and agendaItem 5
        minister Y has assigned agendaItem 4
   Final order of the agendaItems will be: 3 - 5 - 4
*/
async function calculatePriority(exportGraph) {
  const result = parseResult(await query(`
    PREFIX besluitvorming: <http://data.vlaanderen.be/ns/besluitvorming#>
    PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
    PREFIX mandaat: <http://data.vlaanderen.be/ns/mandaat#>
    PREFIX dct: <http://purl.org/dc/terms/>
    PREFIX prov: <http://www.w3.org/ns/prov#>

    SELECT ?newsItem ?number ?mandatee ?title
    WHERE {
      GRAPH ${sparqlEscapeUri(exportGraph)} {
         ?newsItem a besluitvorming:NieuwsbriefInfo .
         ?procedurestap prov:generated ?newsItem ;
             besluitvorming:heeftBevoegde ?mandatee ;
             besluitvorming:isGeagendeerdVia ?agendaItem .
         ?agendaItem ext:prioriteit ?number .
         ?mandatee dct:title ?title .
      }
    }
  `));

  // [ { newsItem, number, mandatee }, ... ]

  // Group results per newsItem
  const uniqueNewsItems = {};
  result.forEach( (r) => {
    const key = r.newsItem;
    uniqueNewsItems[key] = uniqueNewsItems[key] || { number: parseInt(r.number), mandatees: [] };
    if (!uniqueNewsItems[key].mandatees.includes(r.title))
      uniqueNewsItems[key].mandatees.push(r.title);
  });
  console.log(`Found ${Object.keys(uniqueNewsItems).length} news items`);

  // { <news-1>: { number, mandatees }, <news-2>: { number, mandatees }, ... }

  // Create 'unique' key for group of mandatees per item
  const groupPriorities = {};
  const newsItems = [];
  for (let uri in uniqueNewsItems) {
    const item = uniqueNewsItems[uri];
    item.mandatees.sort();
    const groupKey = item.mandatees.join();
    groupPriorities[groupKey] = 0;
    newsItems.push({ uri, groupKey, number: item.number, mandatees: item.mandatees });
  }
  console.log(`Found ${Object.keys(groupPriorities).length} different groups of mandatees`);

  // [ { uri: news-1, groupKey, number, mandatees, ... } ]

  // Determine priority of each group of mandatees based on the lowest agendaitem number assigned to that group
  for (let groupKey in groupPriorities) {
    groupPriorities[groupKey] = Math.min(...newsItems.filter(i => i.groupKey == groupKey).map(i => i.number));
  }
  console.log(`Determined groups of mandatees priorities: ${JSON.stringify(groupPriorities)}`);

  // Order group of mandatees, lowest priority first (= most important)
  const orderedGroupKeys = [];
  for (let groupKey in groupPriorities) {
    orderedGroupKeys.push( { key: groupKey, priority: groupPriorities[groupKey] } );
  }
  orderedGroupKeys.sort( (a, b) => (a.priority > b.priority ? 1 : -1 ));
  console.log(`Sorted groups of mandatees priorities: ${JSON.stringify(orderedGroupKeys)}`);

  // Set overall priority per newsItem based on groupPriority and agendaItem number
  const maxItemNumber = Math.max(...newsItems.map(i => i.number));
  for (let i=0; i < orderedGroupKeys.length; i++) {
    const groupKey = orderedGroupKeys[i].key;
    const baseGroupPriority = i * maxItemNumber;  // make sure priorities between groups don't overlap
    console.log(`Base priority for group ${i} set to ${baseGroupPriority}`);
    newsItems.filter(item => item.groupKey == groupKey).forEach(item => item.priority = baseGroupPriority + item.number);
  }

  // Persist overall priority on newsItem in store
  const triples = newsItems.map( (item) => `<${item.uri}> ext:prioriteit ${sparqlEscapeInt(item.priority)} . ` );

  await update(`
    PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>

    INSERT DATA {
      GRAPH <${exportGraph}> {
        ${triples.join('\n')}
      }
    }
  `);
}

async function getNieuwsbriefInfoFromExport(exportGraph) {
  return await query(`
    PREFIX besluitvorming: <http://data.vlaanderen.be/ns/besluitvorming#>
    PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>

    SELECT ?s
    WHERE {
      GRAPH ${sparqlEscapeUri(exportGraph)} {
        ?s a besluitvorming:NieuwsbriefInfo .
      }
    }
  `);
}

function constructThemeInfo(kaleidosGraph, publicGraph, nieuwsbriefInfo) {
  return `
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
      GRAPH ${sparqlEscapeUri(kaleidosGraph)} {
        ${sparqlEscapeUri(nieuwsbriefInfo.s)} ext:themesOfSubcase ?s .
      }
    }
  `;
}

function constructDocumentsInfo(kaleidosGraph, procedurestapInfo) {
  return `
    PREFIX mu: <http://mu.semte.ch/vocabularies/core/>
    PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
    PREFIX besluitvorming: <http://data.vlaanderen.be/ns/besluitvorming#>
    PREFIX dct: <http://purl.org/dc/terms/>
    PREFIX foaf: <http://xmlns.com/foaf/0.1/>

    CONSTRUCT {
      ?document a foaf:Document ;
        besluitvorming:heeftVersie ?versie ;
        mu:uuid ?uuidDocument ;
        dct:title ?title ;
        ext:documentType ?documentType .
      ?versie a ext:DocumentVersie ;
        mu:uuid ?uuidDocumentVersie ;
        ext:versieNummer ?versieNummer ;
        ext:file ?file .
      ${sparqlEscapeUri(procedurestapInfo.s)} ext:bevatDocumentversie ?versie .
    }
    WHERE {
      GRAPH ${sparqlEscapeUri(kaleidosGraph)} {
        ?document a foaf:Document ;
          ext:toegangsniveauVoorDocument <http://kanselarij.vo.data.gift/id/concept/toegangs-niveaus/6ca49d86-d40f-46c9-bde3-a322aa7e5c8e> ;
          besluitvorming:heeftVersie ?versie ;
          mu:uuid ?uuidDocument .
        OPTIONAL { ?document dct:title ?title . }
        OPTIONAL { ?document ext:documentType ?documentType . }
        ?versie a ext:DocumentVersie ;
          mu:uuid ?uuidDocumentVersie ;
          ext:versieNummer ?versieNummer ;
          ext:file ?file ;
          ^ext:bevatDocumentversie ${sparqlEscapeUri(procedurestapInfo.s)}
      }
    }
  `;
}

async function getDocumentsFromTmp(tmpGraph) {
  return await query(`
    PREFIX foaf: <http://xmlns.com/foaf/0.1/>

    SELECT ?s
    WHERE {
      GRAPH ${sparqlEscapeUri(tmpGraph)} {
        ?s a foaf:Document .
      }
    }
  `);
}

async function constructDocumentsAndLatestVersie(exportGraph, tmpGraph, documentInfo) {
  return await query(`
    PREFIX mu: <http://mu.semte.ch/vocabularies/core/>
    PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
    PREFIX besluitvorming: <http://data.vlaanderen.be/ns/besluitvorming#>
    PREFIX dct: <http://purl.org/dc/terms/>

    INSERT {
      GRAPH ${sparqlEscapeUri(exportGraph)} {
        ${sparqlEscapeUri(documentInfo.s)} a foaf:Document ;
          besluitvorming:heeftVersie ?documentVersie ;
          mu:uuid ?uuidDocument ;
          dct:title ?title ;
          ext:documentType ?documentType .
        ?documentVersie a ext:DocumentVersie ;
          mu:uuid ?uuidDocumentVersie ;
          ext:versieNummer ?versieNummer ;
          ext:file ?file .
      }
    }
    WHERE {
      GRAPH ${sparqlEscapeUri(tmpGraph)} {
        ${sparqlEscapeUri(documentInfo.s)} a foaf:Document ;
          besluitvorming:heeftVersie ?documentVersie ;
          mu:uuid ?uuidDocument .
        OPTIONAL { ?document dct:title ?title . }
        OPTIONAL { ?document ext:documentType ?documentType . }
        ?documentVersie a ext:DocumentVersie ;
          mu:uuid ?uuidDocumentVersie ;
          ext:versieNummer ?versieNummer ;
          ext:file ?file .
      }
    }
    ORDER BY DESC(?versieNummer) LIMIT 1
  `);
}

async function constructLinkNieuwsDocumentVersie(exportGraph, tmpGraph, nieuwsbriefInfo) {
  return await query(`
    PREFIX besluitvorming: <http://data.vlaanderen.be/ns/besluitvorming#>
    PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
    PREFIX prov: <http://www.w3.org/ns/prov#>

    INSERT {
      GRAPH ${sparqlEscapeUri(exportGraph)} {
        ?s ext:documentVersie ?versie .
      }
    }
    WHERE {
      GRAPH ${sparqlEscapeUri(exportGraph)} {
        ?s a besluitvorming:NieuwsbriefInfo ;
          ^prov:generated ?subcase .
      }
      GRAPH ${sparqlEscapeUri(tmpGraph)} {
        ?subcase ext:bevatDocumentversie ?versie .
      }
    }
  `);
}

function constructDocumentTypesInfo(kaleidosGraph, publicGraph, documentInfo) {
  return `
    PREFIX mu: <http://mu.semte.ch/vocabularies/core/>
    PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
    PREFIX skos: <http://www.w3.org/2004/02/skos/core#>

    CONSTRUCT {
      ?documentType a ext:DocumentTypeCode;
        mu:uuid ?uuid ;
        skos:prefLabel ?label .
    }
    WHERE {
      GRAPH ${sparqlEscapeUri(kaleidosGraph)} {
        ${sparqlEscapeUri(documentInfo.s)} a foaf:Document ;
          ext:documentType ?documentType .
      }
      GRAPH ${sparqlEscapeUri(publicGraph)} {
        ?documentType a ext:DocumentTypeCode;
          mu:uuid ?uuid ;
          skos:prefLabel ?label .
      }
    }
  `;
}

async function getDocumentVersiesFromExport(exportGraph) {
  return await query(`
    PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>

    SELECT ?s
    WHERE {
      GRAPH ${sparqlEscapeUri(exportGraph)} {
        ?s a ext:DocumentVersie .
      }
    }
  `);
}

function constructFilesInfo(kaleidosGraph, documentVersieInfo) {
  return `
    PREFIX mu: <http://mu.semte.ch/vocabularies/core/>
    PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
    PREFIX nfo: <http://www.semanticdesktop.org/ontologies/2007/03/22/nfo#>
    PREFIX nie: <http://www.semanticdesktop.org/ontologies/2007/01/19/nie#>
    PREFIX dbpedia: <http://dbpedia.org/ontology/>

    CONSTRUCT {
      ${sparqlEscapeUri(documentVersieInfo.s)} ext:file ?uploadFile .
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
      GRAPH ${sparqlEscapeUri(kaleidosGraph)} {
        ${sparqlEscapeUri(documentVersieInfo.s)} a ext:DocumentVersie ;
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
  `;
}

export {
  parseResult,
  getMeetingUriFromKaleidos,
  constructMeetingInfo,
  constructProcedurestapInfo,
  getProcedurestappenInfoFromTmp,
  constructNieuwsbriefInfo,
  constructLinkZittingNieuws,
  constructMandateeAndPersonInfo,
  getNieuwsbriefInfoFromExport,
  constructThemeInfo,
  constructDocumentsInfo,
  getDocumentsFromTmp,
  constructDocumentsAndLatestVersie,
  constructLinkNieuwsDocumentVersie,
  constructDocumentTypesInfo,
  getDocumentVersiesFromExport,
  constructFilesInfo,
  calculatePriority
}
