import { HawkularUiPage } from './app.po';

describe('hawkular-ui App', () => {
  let page: HawkularUiPage;

  beforeEach(() => {
    page = new HawkularUiPage();
  });

  it('should display welcome message', () => {
    page.navigateTo();
    expect(page.getParagraphText()).toEqual('Welcome to app!!');
  });
});
