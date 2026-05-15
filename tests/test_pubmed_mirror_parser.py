from scripts.pubmed_mirror.parser import PubMedParser


def test_parse_article_extracts_pubmed_fields_and_mesh_annotations():
    xml_text = """
    <PubmedArticleSet>
      <PubmedArticle>
        <MedlineCitation>
          <PMID Version="1">12345</PMID>
          <Article>
            <Journal>
              <JournalIssue>
                <PubDate>
                  <Year>2024</Year>
                  <Month>Jan</Month>
                  <Day>05</Day>
                </PubDate>
              </JournalIssue>
              <Title>Test Journal</Title>
            </Journal>
            <ArticleTitle>Example title</ArticleTitle>
            <AuthorList>
              <Author>
                <ForeName>Jane</ForeName>
                <LastName>Doe</LastName>
              </Author>
            </AuthorList>
            <Language>eng</Language>
            <Language>fre</Language>
            <PublicationTypeList>
              <PublicationType>Journal Article</PublicationType>
              <PublicationType>Research Support, U.S. Gov't</PublicationType>
            </PublicationTypeList>
            <Abstract>
              <AbstractText Label="BACKGROUND">First paragraph.</AbstractText>
              <AbstractText>Second paragraph.</AbstractText>
            </Abstract>
          </Article>
          <MeshHeadingList>
            <MeshHeading>
              <DescriptorName UI="D006041" MajorTopicYN="Y">Goats</DescriptorName>
              <QualifierName UI="Q000235" MajorTopicYN="N">genetics</QualifierName>
              <QualifierName UI="Q000254" MajorTopicYN="Y">growth &amp; development</QualifierName>
            </MeshHeading>
            <MeshHeading>
              <DescriptorName UI="D008892" MajorTopicYN="N">Milk</DescriptorName>
            </MeshHeading>
          </MeshHeadingList>
        </MedlineCitation>
        <PubmedData>
          <PublicationStatus>aheadofprint</PublicationStatus>
          <ArticleIdList>
            <ArticleId IdType="pubmed">12345</ArticleId>
            <ArticleId IdType="doi">10.1000/example</ArticleId>
            <ArticleId IdType="pmc">PMC1234567</ArticleId>
          </ArticleIdList>
        </PubmedData>
      </PubmedArticle>
    </PubmedArticleSet>
    """

    records = PubMedParser.parse_articles(xml_text)

    assert len(records) == 1
    record = records[0]
    assert record.pmid == 12345
    assert record.title == "Example title"
    assert record.journal == "Test Journal"
    assert record.publication_date == "2024-01-05"
    assert record.pub_year == 2024
    assert record.authors == "Jane Doe"
    assert record.abstract == "BACKGROUND: First paragraph.\n\nSecond paragraph."
    assert record.pmc_id == "PMC1234567"
    assert record.doi == "10.1000/example"
    assert record.publication_status == "aheadofprint"
    assert record.publication_type == "Journal Article; Research Support, U.S. Gov't"
    assert record.language == "eng; fre"

    assert len(record.mesh_assignments) == 3
    assert record.mesh_assignments[0].descriptor_ui == "D006041"
    assert record.mesh_assignments[0].descriptor_name == "Goats"
    assert record.mesh_assignments[0].descriptor_major_topic is True
    assert record.mesh_assignments[0].qualifier_ui == "Q000235"
    assert record.mesh_assignments[0].qualifier_name == "genetics"
    assert record.mesh_assignments[0].qualifier_major_topic is False

    assert record.mesh_assignments[1].descriptor_ui == "D006041"
    assert record.mesh_assignments[1].qualifier_ui == "Q000254"
    assert record.mesh_assignments[1].qualifier_name == "growth & development"
    assert record.mesh_assignments[1].qualifier_major_topic is True

    assert record.mesh_assignments[2].descriptor_ui == "D008892"
    assert record.mesh_assignments[2].descriptor_name == "Milk"
    assert record.mesh_assignments[2].descriptor_major_topic is False
    assert record.mesh_assignments[2].qualifier_ui is None
    assert record.mesh_assignments[2].qualifier_name is None
    assert record.mesh_assignments[2].qualifier_major_topic is None
