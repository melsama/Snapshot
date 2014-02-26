require 'spec'

describe Snapshot do
  it 'should be indexing max id' do
    snapshot = Snapshot.new
    snapshot._check_target_max().should == true
  end
end
