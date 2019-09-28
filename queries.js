import { uuid, query, update, sparqlEscapeString, sparqlEscapeUri, sparqlEscapeInt } from 'mu';
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

async function selectMededelingen(kaleidosGraph, meetingUri) {
  return await queryKaleidos(`
  PREFIX mu: <http://mu.semte.ch/vocabularies/core/>
  PREFIX dbpedia: <http://dbpedia.org/ontology/>
  PREFIX dct: <http://purl.org/dc/terms/>
  PREFIX besluitvorming: <http://data.vlaanderen.be/ns/besluitvorming#>
  PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
  PREFIX prov: <http://www.w3.org/ns/prov#>

  SELECT ?agendapunt ?priority ?procedurestap
  WHERE {
    GRAPH ${sparqlEscapeUri(kaleidosGraph)} {
      ${sparqlEscapeUri(meetingUri)} besluitvorming:behandelt ?agenda .
      ?agenda dct:hasPart ?agendapunt .
      ?agendapunt ext:wordtGetoondAlsMededeling "true"^^<http://mu.semte.ch/vocabularies/typed-literals/boolean> ;
                  ext:prioriteit ?priority .

      OPTIONAL {
        ?procedurestap a dbpedia:UnitOfWork ;
          mu:uuid ?uuid ;
          besluitvorming:isGeagendeerdVia ?agendapunt ;
          prov:generated ?nieuwsbriefInfo .
        ?nieuwsbriefInfo a besluitvorming:NieuwsbriefInfo ;
          ext:afgewerkt \"true\"^^<http://mu.semte.ch/vocabularies/typed-literals/boolean> .
      }
    }
  }`);
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
          ext:wordtGetoondAlsMededeling "false"^^<http://mu.semte.ch/vocabularies/typed-literals/boolean> ;
          besluitvorming:heeftBevoegde ?heeftBevoegde .
      }
    }
  `);
}

function constructNieuwsbriefInfoForProcedurestap(kaleidosGraph, procedurestapUri, category = "nieuws") {
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
        ext:themesOfSubcase ?themesOfSubcase ;
        ext:newsItemCategory ${sparqlEscapeString(category)} .
      ${sparqlEscapeUri(procedurestapUri)} besluitvorming:heeftBevoegde ?heeftBevoegde ;
        prov:generated ?s ;
        besluitvorming:isGeagendeerdVia ?agendapunt .
      ?agendapunt ext:prioriteit ?priority .
    }
    WHERE {
      GRAPH ${sparqlEscapeUri(kaleidosGraph)} {
        ${sparqlEscapeUri(procedurestapUri)} prov:generated ?s .
        ${sparqlEscapeUri(procedurestapUri)} besluitvorming:isGeagendeerdVia ?agendapunt .
        ?agendapunt ext:prioriteit ?priority .
        ?s a besluitvorming:NieuwsbriefInfo ;
          ext:afgewerkt \"true\"^^<http://mu.semte.ch/vocabularies/typed-literals/boolean> ;
          mu:uuid ?uuid ;
          dct:title ?title ;
          ext:htmlInhoud ?htmlInhoud .
        OPTIONAL { ?s ext:themesOfSubcase ?themesOfSubcase .}
        OPTIONAL { ${sparqlEscapeUri(procedurestapUri)} besluitvorming:heeftBevoegde ?heeftBevoegde . }
      }
    }
  `;
}

function constructNieuwsbriefInfoForAgendapunt(kaleidosGraph, agendapuntUri, category = "mededeling") {
  const newsUuid = uuid();
  const newsUri = `http://kanselarij.vo.data.gift/nieuwsbrief-infos/${newsUuid}`;

  return `
    PREFIX mu: <http://mu.semte.ch/vocabularies/core/>
    PREFIX dct: <http://purl.org/dc/terms/>
    PREFIX besluitvorming: <http://data.vlaanderen.be/ns/besluitvorming#>
    PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
    PREFIX prov: <http://www.w3.org/ns/prov#>
    PREFIX besluit: <http://data.vlaanderen.be/ns/besluit#>

    CONSTRUCT {
      ${sparqlEscapeUri(agendapuntUri)} prov:generated ${sparqlEscapeUri(newsUri)} .
      ${sparqlEscapeUri(newsUri)} a besluitvorming:NieuwsbriefInfo ;
        mu:uuid ${sparqlEscapeString(newsUuid)} ;
        dct:title ?title ;
        ext:htmlInhoud ?content ;
        ext:mededelingPrioriteit ?priority ;
        ext:newsItemCategory ${sparqlEscapeString(category)} .
    }
    WHERE {
      GRAPH ${sparqlEscapeUri(kaleidosGraph)} {
        ${sparqlEscapeUri(agendapuntUri)} a besluit:Agendapunt ;
          dct:title ?content ;
          ext:prioriteit ?priority .
        OPTIONAL { ${sparqlEscapeUri(agendapuntUri)} dct:alternative ?shortTitle }
        BIND(COALESCE(?shortTitle, ?content) as ?title)
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

function constructMandateeAndPersonInfo(kaleidosGraph, mandateeUri) {
  return `
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
        ${sparqlEscapeUri(mandateeUri)} a mandaat:Mandataris ;
          mu:uuid ?uuidMandatee ;
          dct:title ?title ;
          mandaat:start ?start ;
          mandaat:isBestuurlijkeAliasVan ?person .
        OPTIONAL { ${sparqlEscapeUri(mandateeUri)} mandaat:einde ?end . }
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
async function calculatePriorityNewsItems(exportGraph) {
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

/*
  Mededeling are listed in order of the agendapunten after the news items.
*/
async function calculatePriorityMededelingen(exportGraph) {
  const results = parseResult(await query(`
    PREFIX besluitvorming: <http://data.vlaanderen.be/ns/besluitvorming#>
    PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>

    SELECT ?newsItem ?priority
    WHERE {
      GRAPH ${sparqlEscapeUri(exportGraph)} {
         ?newsItem a besluitvorming:NieuwsbriefInfo ;
            ext:mededelingPrioriteit ?priority .
      }
    }
  `));

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
  };
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

function constructDocumentsInfoForProcedurestap(kaleidosGraph, procedurestapUri) {
  return constructDocumentsInfo(kaleidosGraph, 'ext:bevatDocumentversie', procedurestapUri);
}

function constructDocumentsInfoForAgendapunt(kaleidosGraph, agendapuntUri) {
  return constructDocumentsInfo(kaleidosGraph, 'ext:bevatAgendapuntDocumentversie', agendapuntUri);
}

function constructDocumentsInfo(kaleidosGraph, documentVersiePredicate, resourceUri) {
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
        ext:toegangsniveauVoorDocumentVersie ?accessLevel ;
        ext:file ?file .
      ${sparqlEscapeUri(resourceUri)} ext:bevatDocumentversie ?versie .
    }
    WHERE {
      GRAPH ${sparqlEscapeUri(kaleidosGraph)} {
        ${sparqlEscapeUri(resourceUri)} ${documentVersiePredicate} ?versie .
        ?versie a ext:DocumentVersie ;
          mu:uuid ?uuidDocumentVersie ;
          ext:versieNummer ?versieNummer ;
          ext:toegangsniveauVoorDocumentVersie ?accessLevel ;
          ext:file ?file .
        ?document a foaf:Document ;
          besluitvorming:heeftVersie ?versie ;
          mu:uuid ?uuidDocument .
        OPTIONAL { ?document dct:title ?title . }
        OPTIONAL { ?document ext:documentType ?documentType . }
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

async function getLastVersieAccessLevel(tmpGraph, documentInfo) {
  return await query(`
    PREFIX mu: <http://mu.semte.ch/vocabularies/core/>
    PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
    PREFIX besluitvorming: <http://data.vlaanderen.be/ns/besluitvorming#>
    PREFIX dct: <http://purl.org/dc/terms/>

    SELECT ?accessLevel
    WHERE {
      GRAPH ${sparqlEscapeUri(tmpGraph)} {
        ${sparqlEscapeUri(documentInfo.s)} a foaf:Document ;
          besluitvorming:heeftVersie ?documentVersie .
        ?documentVersie a ext:DocumentVersie ;
          ext:toegangsniveauVoorDocumentVersie ?accessLevel ;
          ext:versieNummer ?versieNummer .
      }
    }
    ORDER BY DESC(?versieNummer) LIMIT 1
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
        OPTIONAL { ${sparqlEscapeUri(documentInfo.s)} dct:title ?title . }
        OPTIONAL { ${sparqlEscapeUri(documentInfo.s)} ext:documentType ?documentType . }
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
        ?newsInfo ext:documentVersie ?versie .
      }
    }
    WHERE {
      GRAPH ${sparqlEscapeUri(exportGraph)} {
        ?subCaseOrAgendapunt prov:generated ?newsInfo .
        ?newsInfo a besluitvorming:NieuwsbriefInfo .
      }
      GRAPH ${sparqlEscapeUri(tmpGraph)} {
        ?subCaseOrAgendapunt ext:bevatDocumentversie ?versie .
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
  constructProcedurestapInfo,
  constructMeetingInfo,
  constructNieuwsbriefInfoForProcedurestap,
  constructNieuwsbriefInfoForAgendapunt,
  getProcedurestappenInfoFromTmp,
  selectMededelingen,
  constructLinkZittingNieuws,
  constructMandateeAndPersonInfo,
  getNieuwsbriefInfoFromExport,
  constructThemeInfo,
  constructDocumentsInfoForProcedurestap,
  constructDocumentsInfoForAgendapunt,
  getDocumentsFromTmp,
  getLastVersieAccessLevel,
  constructDocumentsAndLatestVersie,
  constructLinkNieuwsDocumentVersie,
  constructDocumentTypesInfo,
  getDocumentVersiesFromExport,
  constructFilesInfo,
  calculatePriorityNewsItems,
  calculatePriorityMededelingen
}
